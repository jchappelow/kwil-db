package execution

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/kwilteam/kwil-db/internal/sql/pg"
)

// baseDataset is a deployed database schema.
// It implements the precompiles.Instance interface.
type baseDataset struct {
	// schema is the schema of the dataset.
	schema *types.Schema

	// extensions are the extensions available for use in the dataset.
	extensions map[string]precompiles.Instance

	// actions are the actions that are available for use in the dataset.
	actions map[string]*preparedAction

	// procedures are the procedures that are available for use in the dataset.
	// It only includes public procedures.
	procedures map[string]*preparedProcedure

	// global is the global context.
	global *GlobalContext

	// These support the Catalog. Probably move these up to global context.
	// stats  map[string]*datatypes.Statistics
	// fields map[string]*datatypes.Schema
	dirty bool

	// precomputed refresh height modulo offset
	mod int64
}

var _ precompiles.Instance = (*baseDataset)(nil)

var (
	ErrPrivate   = fmt.Errorf("procedure/action is not public")
	ErrOwnerOnly = fmt.Errorf("procedure/action is owner only")
)

func bytesModN(h []byte, n int64) int64 {
	x := big.NewInt(0).SetBytes(h[:])
	return bigModN(x, n)
}

// (int64(h) + o) mod n
func bytesModN2(h []byte, o, n int64) int64 {
	// x = x.Add(x, big.NewInt(o)); return bigModN(x, n)
	// (a + b) mod n = [(a mod n) + (b mod n)] mod n
	return (bytesModN(h, n) + (o % n)) % n
}

func bigModN(x *big.Int, n int64) int64 {
	return x.Mod(x, big.NewInt(n)).Int64()
}

func sqlSchema(table *types.Table, tblRef *sql.TableRef) *sql.Schema {
	tableFields := make([]sql.Field, len(table.Columns))
	for i, col := range table.Columns {
		dataType := col.Type.Name // TODO: convert or standardize across
		nullable := !col.HasAttribute(types.NOT_NULL)
		tableFields[i] = sql.NewFieldWithRelation(col.Name, dataType, nullable, tblRef)
	}
	return &sql.Schema{Fields: tableFields}
}

// buildStats refreshes/builds statistics for the tables in the dataset. These
// statistics are used to implement the Catalog used by the query planner for
// cost estimation.
//
// After initial population of the statistics from a full table scan, the
// statistics must be updated for efficiently. TODO: figure out what column
// stats can be updated, and which if any need to use the DB transaction's
// changeset to reestablish completely accurate stats
func buildStats(ctx context.Context, schema *types.Schema, db sql.Executor,
	catalog *catalog /*meta map[sql.TableRef]*tableMeta*/) (map[sql.TableRef]*sql.Statistics, error) {
	dbid := schema.DBID()
	pgSchema := dbidSchema(dbid)

	fresh := make(map[sql.TableRef]*sql.Statistics, len(schema.Tables))
	for _, table := range schema.Tables {
		// qualifiedTable := pgSchema + "." + table.Name
		stats, err := pg.TableStats(ctx, pgSchema, table.Name, db)
		if err != nil {
			return nil, err
		}

		tableRef := sql.TableRef{
			Namespace: pgSchema, // actually "ds_<dbid>"
			Table:     table.Name,
		}

		catalog.fields[tableRef] = sqlSchema(table, &tableRef)
		catalog.stats[tableRef] = stats
		fresh[tableRef] = stats
		// catalog.meta[tableRef] = &tableMeta{costSchema, stats}
	}
	return fresh, nil
}

// Call calls a procedure from the dataset.
// If the procedure is not public, it will return an error.
// It satisfies precompiles.Instance.
func (d *baseDataset) Call(caller *precompiles.ProcedureContext, app *common.App, method string, inputs []any) ([]any, bool, error) {
	// check if it is a procedure
	proc, ok := d.procedures[method]
	if ok {
		if !proc.public {
			return nil, false, fmt.Errorf(`%w: "%s"`, ErrPrivate, method)
		}
		if proc.ownerOnly && !bytes.Equal(caller.Signer, d.schema.Owner) {
			return nil, false, fmt.Errorf(`%w: "%s"`, ErrOwnerOnly, method)
		}
		mutative := !proc.view
		if mutative && app.DB.(sql.AccessModer).AccessMode() == sql.ReadOnly {
			return nil, false, fmt.Errorf(`%w: "%s"`, ErrMutativeProcedure, method)
		}

		// this is not a strictly necessary check, as postgres will throw an error, but this gives a more
		// helpful error message
		if len(inputs) != len(proc.parameters) {
			return nil, false, fmt.Errorf(`procedure "%s" expects %d argument(s), got %d`, method, len(proc.parameters), len(inputs))
		}

		res, err := app.DB.Execute(caller.Ctx, proc.callString(d.schema.DBID()), append([]any{pg.QueryModeExec}, inputs...)...)
		if err != nil {
			return nil, false, err
		}

		err = proc.shapeReturn(res)
		if err != nil {
			return nil, false, err
		}

		caller.Result = res
		return nil, mutative, nil
	}

	// otherwise, it is an action
	act, ok := d.actions[method]
	if !ok {
		return nil, false, fmt.Errorf(`action "%s" not found`, method)
	}

	if !act.public {
		return nil, false, fmt.Errorf(`%w: "%s"`, ErrPrivate, method)
	}
	mutative := !act.view
	if !act.view && app.DB.(sql.AccessModer).AccessMode() == sql.ReadOnly {
		return nil, mutative, fmt.Errorf(`%w: "%s"`, ErrMutativeProcedure, method)
	}

	newCtx := caller.NewScope()
	newCtx.DBID = d.schema.DBID()
	newCtx.Procedure = method

	err := act.call(newCtx, d.global, app.DB, inputs)
	if err != nil {
		return nil, false, err
	}

	caller.Result = newCtx.Result

	// we currently do not support returning values from dataset procedures
	// if we do, then we will need to return the result here
	return nil, mutative, nil
}
