package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/core/utils/order"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/kwilteam/kwil-db/internal/engine/generate"
	"github.com/kwilteam/kwil-db/internal/sql/pg"
	"github.com/kwilteam/kwil-db/internal/sql/versioning"
	"github.com/kwilteam/kwil-db/parse"
)

// GlobalContext is the context for the entire execution.
// It exists for the lifetime of the server.
// It stores information about deployed datasets in-memory, and provides methods to interact with them.
type GlobalContext struct {
	// mu protects the datasets maps, which is written to during block execution
	// and read from during calls / queries.
	// It also implicitly protects maps held in the *baseDataset struct.
	mu sync.RWMutex

	// initializers are the namespaces that are available to datasets.
	// This includes other datasets, or loaded extensions.
	initializers map[string]precompiles.Initializer

	// datasets are the top level namespaces that are available to engine callers.
	// These only include datasets, and do not include extensions.
	datasets map[string]*baseDataset

	service *common.Service

	catalog catalog // dt.Catalog

	fs FS
}

type FS interface {
	// fs.FS
	ReadFile(name string) ([]byte, error)
	WriteFile(name string, data []byte) error
}

type tableMeta struct {
	schema *sql.Schema
	stats  *sql.Statistics
}

func (tm tableMeta) Statistics() *sql.Statistics {
	return tm.stats
}
func (tm tableMeta) Schema() *sql.Schema {
	return tm.schema
}

var _ sql.DataSource = tableMeta{}
var _ sql.DataSource = (*tableMeta)(nil)

type catalog struct {
	fields map[sql.TableRef]*sql.Schema
	stats  map[sql.TableRef]*sql.Statistics
	// meta map[sql.TableRef]*tableMeta
}

// var _ query_planner.Catalog = (*catalog)(nil)

func (m *catalog) GetDataSource(tableRef *sql.TableRef) (sql.DataSource, error) {
	schema, ok := m.fields[*tableRef]
	if !ok {
		return nil, errors.New("table schema not found in catalog")
	}
	stats, ok := m.stats[*tableRef]
	if !ok {
		return nil, errors.New("table stats not found in catalog")
	}
	return &tableMeta{schema, stats}, nil
}

var (
	ErrDatasetNotFound = errors.New("dataset not found")
	ErrDatasetExists   = errors.New("dataset exists")
	ErrInvalidSchema   = errors.New("invalid schema")
	ErrDBInternal      = errors.New("internal database error")
)

func InitializeEngine(ctx context.Context, tx sql.DB) error {
	upgradeFns := map[int64]versioning.UpgradeFunc{
		0: initTables,
		1: func(ctx context.Context, db sql.DB) error {

			// add the uuid column to the kwil_schemas table
			_, err := db.Execute(ctx, sqlUpgradeSchemaTableV1AddUUIDColumn)
			if err != nil {
				return err
			}

			// backfill the uuid column with uuids
			_, err = db.Execute(ctx, sqlBackfillSchemaTableV1UUID)
			if err != nil {
				return err
			}

			// remove the primary key constraint from the kwil_schemas table
			_, err = db.Execute(ctx, sqlUpgradeRemovePrimaryKey)
			if err != nil {
				return err
			}

			// add the new primary key constraint to the kwil_schemas table
			_, err = db.Execute(ctx, sqlUpgradeAddPrimaryKeyV1UUID)
			if err != nil {
				return err
			}

			// add a unique constraint to the dbid column
			_, err = db.Execute(ctx, sqlUpgradeAddUniqueConstraintV1DBID)
			if err != nil {
				return err
			}

			_, err = db.Execute(ctx, sqlUpgradeSchemaTableV1AddOwnerColumn)
			if err != nil {
				return err
			}

			_, err = db.Execute(ctx, sqlUpgradeSchemaTableV1AddNameColumn)
			if err != nil {
				return err
			}

			// we now need to read out all schemas to backfill the changes to
			// the datasets table. This includes:
			// - upgrading the version of the schema
			// - setting the owner of the schema
			// - setting the name of the schema
			schemas, err := getSchemas(ctx, db, convertV07Schema)
			if err != nil {
				return err
			}

			for _, schema := range schemas {
				bts, err := json.Marshal(schema)
				if err != nil {
					return err
				}

				_, err = db.Execute(ctx, sqlBackfillSchemaTableV1, schema.Owner, schema.Name, schema.DBID(), bts)
				if err != nil {
					return err
				}
			}

			_, err = db.Execute(ctx, sqlAddProceduresTableV1)
			if err != nil {
				return err
			}

			_, err = db.Execute(ctx, sqlIndexProceduresTableV1SchemaID)
			if err != nil {
				return err
			}

			return nil
		},
	}

	err := versioning.Upgrade(ctx, tx, pg.InternalSchemaName, upgradeFns, engineVersion)
	if err != nil {
		return err
	}

	return nil
}

// NewGlobalContext creates a new global context. It will load any persisted
// datasets from the datastore. The provided database is only used for
// construction.
func NewGlobalContext(ctx context.Context, db sql.Executor, extensionInitializers map[string]precompiles.Initializer,
	service *common.Service, fs FS) (*GlobalContext, error) {
	g := &GlobalContext{
		initializers: extensionInitializers,
		datasets:     make(map[string]*baseDataset),
		service:      service,
		catalog: catalog{
			fields: map[sql.TableRef]*sql.Schema{},
			stats:  map[sql.TableRef]*sql.Statistics{},
		},
		fs: fs,
	}

	schemas, err := getSchemas(ctx, db, nil)
	if err != nil {
		return nil, err
	}

	// we need to make sure schemas are ordered by their dependencies
	// if one schema is dependent on another, it must be loaded after the other
	// this is handled by the orderSchemas function
	for _, schema := range orderSchemas(schemas) {
		dbid := schema.DBID()
		_, err := g.loadDataset(ctx, schema)
		if err != nil {
			return nil, fmt.Errorf("%w: schema (%s / %s / %s)", err, schema.Name, dbid, schema.Owner)
		}

		// load up the table+col stats

		schemaStatsBts, err := g.fs.ReadFile(dbid)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			g.service.Logger.S.Warnf("sql table statistics not found for %v, rebuilding", dbid)
			err := g.buildStats(ctx, schema, db) // sets g.catalog.{fields,stats}
			if err != nil {
				return nil, err
			}
		} else {
			schemaStats, err := pg.DecStats(bytes.NewReader(schemaStatsBts))
			if err != nil {
				return nil, err
			}

			for tblRef, stats := range schemaStats {
				g.service.Logger.S.Infof("loaded statistics for table %v", tblRef)
				tbl, found := schema.FindTable(tblRef.Table)
				if !found {
					return nil, fmt.Errorf("table not found in loaded SQL stats: %v", tblRef.Table)
				}
				g.catalog.fields[tblRef] = sqlSchema(tbl, &tblRef)
				g.catalog.stats[tblRef] = stats
			}
		}
	}

	if pgDB, is := db.(interface {
		SetBaseStats(map[sql.TableRef]*sql.Statistics)
	}); is {
		pgDB.SetBaseStats(g.catalog.stats)
	}

	return g, nil
}

func (g *GlobalContext) DBStats() map[sql.TableRef]*sql.Statistics {
	return g.catalog.stats
}

const statsRefreshModulus = 20

// RebuildStatistics rebuilds column statistics for relations in schemas that
// are scheduled for a refresh according to the current height, which is just a
// monotonically increasing sequence for this purpose.  This also must persist
// *all* statistics (or at least all statistics that were updated since the
// previous call to this method) so that on restart the node's view of
// statistics is consistent with other nodes, which would not be the case if a
// full scan of every table were performed on node startup since the incremental
// updates that occur at every block eventually create stale statistics.
//
// dang, the incremental updates occur on Precommit presently, so a rebuild that
// reflects block X should not be followed by the incremental updates for block X.
//   - opt: skip the incremental updates with a flag somehow, yuk
//   - opt: do rebuild at the *beginning* of a block, before tx exc
//   - opt: do rebuild outside of the consensus tx, after Commit, perhaps async/background with next block able to wait on it
func (g *GlobalContext) RebuildStatistics(ctx context.Context, height int64, db sql.Executor) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// schemas, err := getSchemas(ctx, db, nil) // from disk, then g.loadDataset
	for _, pair /*dbid, dataset*/ := range order.OrderMap(g.datasets) { // for loaded datasets
		dbid, dataset := pair.Key, pair.Value
		schema := dataset.schema

		if !dataset.dirty {
			g.service.Logger.S.Infof("Skipping rebuild of stats for unchanged dataset %s (%s), mod %d",
				schema.Name, dbid, dataset.mod)
			continue
		}

		// rebuild is periodic based on dbid
		//  identity: (a + b) mod n = [(a mod n) + (b mod n)] mod n
		if ((dataset.mod + (height % statsRefreshModulus)) % statsRefreshModulus) != 0 {
			g.service.Logger.S.Infof("Skipping rebuild of stats for changed dataset %s (%s), mod %d",
				schema.Name, dbid, dataset.mod)
			// But store it since it has updates
			schemaStats := g.extractStats(schema)
			if err := g.storeStats(dbid, schemaStats); err != nil {
				g.service.Logger.S.Errorf("storing schema stats (%d) failed: %v", dbid, err)
			}
			continue
		}

		g.service.Logger.S.Infof("Refreshing table statistics for database %s (%s), mod %d",
			schema.Name, dbid, dataset.mod)
		if err := g.buildStats(ctx, schema, db); err != nil {
			return err
		}
		dataset.dirty = false
	}

	return nil
}

func (g *GlobalContext) extractStats(schema *types.Schema) map[sql.TableRef]*sql.Statistics {
	schemaStats := make(map[sql.TableRef]*sql.Statistics, len(schema.Tables))
	dbid := schema.DBID()
	pgSchema := dbidSchema(dbid)
	for _, table := range schema.Tables {
		tableRef := sql.TableRef{
			Namespace: pgSchema,
			Table:     table.Name,
		}
		schemaStats[tableRef] = g.catalog.stats[tableRef]
	}
	return schemaStats
}

// Reload is used to reload the global context based on the current state of the database.
// It is used after state sync to ensure that the global context is up to date.
func (g *GlobalContext) Reload(ctx context.Context, db sql.Executor) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	schemas, err := getSchemas(ctx, db, nil)
	if err != nil {
		return err
	}

	for _, schema := range orderSchemas(schemas) {
		_, err := g.loadDataset(ctx, schema)
		if err != nil {
			return err
		}
		if err = g.buildStats(ctx, schema, db); err != nil {
			return err
		}
	}

	return nil
}

// CreateDataset deploys a schema.
// It will create the requisite tables, and perform the required initializations.
func (g *GlobalContext) CreateDataset(ctx context.Context, tx sql.DB, schema *types.Schema, txdata *common.TransactionData) (err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	err = schema.Clean()
	if err != nil {
		return errors.Join(err, ErrInvalidSchema)
	}
	schema.Owner = txdata.Signer

	_, err = g.loadDataset(ctx, schema)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			g.unloadDataset(schema.DBID())
		}
	}()

	// it is critical that the schema is loaded before being created.
	// the engine will not be able to parse the schema if it is not loaded.
	err = createSchema(ctx, tx, schema, txdata.TxID)
	if err != nil {
		return err
	}

	return g.buildStats(ctx, schema, tx)
}

// DeleteDataset deletes a dataset.
// It will ensure that the caller is the owner of the dataset.
func (g *GlobalContext) DeleteDataset(ctx context.Context, tx sql.DB, dbid string, txdata *common.TransactionData) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	dataset, ok := g.datasets[dbid]
	if !ok {
		return ErrDatasetNotFound
	}

	if !bytes.Equal(txdata.Signer, dataset.schema.Owner) {
		return fmt.Errorf(`cannot delete dataset "%s", not owner`, dbid)
	}

	err := deleteSchema(ctx, tx, dataset.schema)
	if err != nil {
		return errors.Join(err, ErrDBInternal)
	}

	g.unloadDataset(dbid)

	return nil
}

// Procedure calls a procedure on a dataset. It can be given either a readwrite or
// readonly transaction. If it is given a read-only transaction, it will not be
// able to execute any procedures that are not `view`.
func (g *GlobalContext) Procedure(ctx context.Context, tx sql.DB, options *common.ExecutionData) (*sql.ResultSet, error) {
	err := options.Clean()
	if err != nil {
		return nil, err
	}

	g.mu.RLock() // even if tx is readwrite, we will not change GlobalContext state, so we can use RLock
	defer g.mu.RUnlock()

	dataset, ok := g.datasets[options.Dataset]
	if !ok {
		return nil, ErrDatasetNotFound
	}

	procedureCtx := &precompiles.ProcedureContext{
		Ctx:       ctx,
		Signer:    options.Signer,
		Caller:    options.Caller,
		DBID:      options.Dataset,
		Procedure: options.Procedure,
		TxID:      options.TxID,
		Height:    options.Height,
		// starting with stack depth 0, increment in each action call
		GasLimit: 10000000,
	}

	tx2, err := tx.BeginTx(ctx)
	if err != nil {
		return nil, errors.Join(err, ErrDBInternal)
	}
	defer tx2.Rollback(ctx)

	err = setContextualVars(ctx, tx2, options)
	if err != nil {
		return nil, err
	}

	_, mutative, err := dataset.Call(procedureCtx, &common.App{
		Service: g.service,
		DB:      tx2,
		Engine:  g,
	}, options.Procedure, options.Args)
	if err != nil {
		return nil, err
	}

	err = tx2.Commit(ctx)
	if err == nil && mutative {
		dataset.dirty = true
	} // else it will rollback all changes

	return procedureCtx.Result, err
}

// ListDatasets list datasets deployed by a specific caller.
// If caller is empty, it will list all datasets.
func (g *GlobalContext) ListDatasets(caller []byte) ([]*types.DatasetIdentifier, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var datasets []*types.DatasetIdentifier
	if len(caller) == 0 { // prealloc only for all users' dataset
		datasets = make([]*types.DatasetIdentifier, 0, len(g.datasets))
	}
	for dbid, dataset := range g.datasets {
		if len(caller) == 0 || bytes.Equal(dataset.schema.Owner, caller) {
			datasets = append(datasets, &types.DatasetIdentifier{
				Name:  dataset.schema.Name,
				Owner: dataset.schema.Owner,
				DBID:  dbid,
			})
		}
	}

	return datasets, nil
}

// GetSchema gets a schema from a deployed dataset.
func (g *GlobalContext) GetSchema(dbid string) (*types.Schema, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	dataset, ok := g.datasets[dbid]
	if !ok {
		return nil, ErrDatasetNotFound
	}

	return dataset.schema, nil
}

// Execute executes a SQL statement on a dataset. If the statement is mutative,
// the tx must also be a sql.AccessModer. It uses Kwil's SQL dialect.
func (g *GlobalContext) Execute(ctx context.Context, tx sql.DB, dbid, query string, values map[string]any) (*sql.ResultSet, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	dataset, ok := g.datasets[dbid]
	if !ok {
		return nil, ErrDatasetNotFound
	}

	// errLis := parseTypes.NewErrorListener()

	// // We have to parse the query and ensure the dbid is used to derive schema.
	// // OR do we permit (or require) the schema to be specified in the query? It
	// // could go either way, but this ad hoc query function is questionable anyway.
	// parsed, err := sqlanalyzer.ApplyRules(query,
	// 	sqlanalyzer.AllRules,
	// 	dataset.schema, dbidSchema(dbid), errLis)
	// if err != nil {
	// 	return nil, err
	// }
	// if errLis.Err() != nil {
	// 	return nil, errLis.Err()
	// }
	res, err := parse.ParseSQL(query, dataset.schema, false)
	if err != nil {
		return nil, err
	}

	if res.ParseErrs.Err() != nil {
		return nil, res.ParseErrs.Err()
	}

	sqlStmt, params, err := generate.WriteSQL(res.AST, true, dbidSchema(dbid))
	if err != nil {
		return nil, err
	}

	if res.Mutative {
		txm, ok := tx.(sql.AccessModer)
		if !ok {
			return nil, errors.New("DB does not provide access mode needed for mutative statement")
		}
		if txm.AccessMode() == sql.ReadOnly {
			return nil, fmt.Errorf("cannot execute a mutative query in a read-only transaction")
		}
	}

	args := orderAndCleanValueMap(values, params)
	args = append([]any{pg.QueryModeExec}, args...)

	result, err := tx.Execute(ctx, sqlStmt, args...)
	if err != nil {
		return nil, decorateExecuteErr(err, query)
	}

	if res.Mutative && result.Status.RowsAffected > 0 {
		dataset.dirty = true
	}

	return result, nil
}

type dbQueryFn func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error)

func (g *GlobalContext) buildStats(ctx context.Context, schema *types.Schema, db sql.Executor) error {
	dbid := schema.DBID()

	schemaStats, err := buildStats(ctx, schema, db, &g.catalog)
	if err != nil {
		return err
	}

	return g.storeStats(dbid, schemaStats)
}

func (g *GlobalContext) storeStats(dbid string, schemaStats map[sql.TableRef]*sql.Statistics) error {
	var buf bytes.Buffer
	if err := pg.EncStats(&buf, schemaStats); err != nil {
		return err
	}
	return g.fs.WriteFile(dbid, buf.Bytes())
}

// loadDataset loads a dataset into the global context.
// It does not create the dataset in the datastore.
func (g *GlobalContext) loadDataset(ctx context.Context, schema *types.Schema) (*baseDataset, error) { //nolint:unparam
	dbid := schema.DBID()
	_, ok := g.initializers[dbid]
	if ok {
		return nil, fmt.Errorf("%w: %s", ErrDatasetExists, dbid)
	}

	datasetCtx := &baseDataset{
		schema:     schema,
		extensions: make(map[string]precompiles.Instance),
		actions:    make(map[string]*preparedAction),
		procedures: make(map[string]*preparedProcedure),
		global:     g,
		mod:        bytesModN([]byte(dbid[1:]), statsRefreshModulus),
		dirty:      true, // perform a rescan incase the node is loading an existing dataset
	}

	preparedActions, err := prepareActions(schema)
	if err != nil {
		return nil, errors.Join(err, ErrInvalidSchema)
	}

	for _, prepared := range preparedActions {
		_, ok := datasetCtx.actions[prepared.name]
		if ok {
			return nil, fmt.Errorf(`%w: duplicate action name: "%s"`, ErrInvalidSchema, prepared.name)
		}

		datasetCtx.actions[prepared.name] = prepared
	}

	for _, unprepared := range schema.Procedures {
		prepared, err := prepareProcedure(unprepared)
		if err != nil {
			return nil, errors.Join(err, ErrInvalidSchema)
		}

		_, ok := datasetCtx.procedures[prepared.name]
		if ok {
			return nil, fmt.Errorf(`%w: duplicate procedure name: "%s"`, ErrInvalidSchema, prepared.name)
		}

		datasetCtx.procedures[prepared.name] = prepared
	}

	for _, ext := range schema.Extensions {
		_, ok := datasetCtx.extensions[ext.Alias]
		if ok {
			return nil, fmt.Errorf(`%w duplicate namespace assignment: "%s"`, ErrInvalidSchema, ext.Alias)
		}

		initializer, ok := g.initializers[ext.Name]
		if !ok {
			return nil, fmt.Errorf(`namespace "%s" not found`, ext.Name) // ErrMissingExtension?
		}

		namespace, err := initializer(&precompiles.DeploymentContext{
			Ctx:    ctx,
			Schema: schema,
		}, g.service, ext.CleanMap())
		if err != nil {
			return nil, err
		}

		datasetCtx.extensions[ext.Alias] = namespace
	}

	g.initializers[dbid] = func(_ *precompiles.DeploymentContext, _ *common.Service, _ map[string]string) (precompiles.Instance, error) {
		return datasetCtx, nil
	}
	g.datasets[dbid] = datasetCtx

	return datasetCtx, nil
}

// unloadDataset unloads a dataset from the global context.
// It does not delete the dataset from the datastore.
func (g *GlobalContext) unloadDataset(dbid string) {
	// perform stats cleanup
	dataset := g.datasets[dbid]
	pgSchema := dbidSchema(dbid)
	for _, tbl := range dataset.schema.Tables {
		ref := sql.TableRef{Namespace: pgSchema, Table: tbl.Name}
		// delete(g.catalog.stats, ref)

		// This ^ is not optimal as stats updates are ideally done in ONE place.
		// Also, this would happen when the tx is executed PRIOR to the
		// streaming updates that create the changeset and do other statistics updates.

		g.service.Logger.S.Infof("unloading dataset for table %v", ref)
	}

	delete(g.datasets, dbid)
	delete(g.initializers, dbid)
}

// orderSchemas orders schemas based on their dependencies to other schemas.
func orderSchemas(schemas []*types.Schema) []*types.Schema {
	// Mapping from schema DBID to its extensions
	schemaMap := make(map[string][]string)
	for _, schema := range schemas {
		var exts []string
		for _, ext := range schema.Extensions {
			exts = append(exts, ext.Name)
		}
		schemaMap[schema.DBID()] = exts
	}

	// Topological sort
	var result []string
	visited := make(map[string]bool)
	var visitAll func(items []string)

	visitAll = func(items []string) {
		for _, item := range items {
			if !visited[item] {
				visited[item] = true
				visitAll(schemaMap[item])
				result = append(result, item)
			}
		}
	}

	keys := make([]string, 0, len(schemaMap))
	for key := range schemaMap {
		keys = append(keys, key)
	}
	sort.Strings(keys) // sort the keys for deterministic output
	visitAll(keys)

	// Reorder schemas based on result
	var orderedSchemas []*types.Schema
	for _, dbid := range result {
		for _, schema := range schemas {
			if schema.DBID() == dbid {
				orderedSchemas = append(orderedSchemas, schema)
				break
			}
		}
	}

	return orderedSchemas
}
