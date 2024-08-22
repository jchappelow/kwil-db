//go:build pglive

// package integration_test contains full engine integration tests
package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/log"
	actions "github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/kwilteam/kwil-db/internal/conv"
	"github.com/kwilteam/kwil-db/internal/engine/execution"
	"github.com/kwilteam/kwil-db/internal/sql/pg"
)

func TestMain(m *testing.M) {
	// pg.UseLogger(log.NewStdOut(log.InfoLevel)) // uncomment for debugging
	os.Exit(m.Run())
}

// cleanup deletes all schemas and closes the database
func cleanup(t *testing.T, db *pg.DB) {
	txCounter = 0 // reset the global tx counter, which is necessary to properly
	// encapsulate each test and make their results independent of each other

	defer db.Close()
	rmDatasets(t, db)

	db.AutoCommit(true)
	defer db.AutoCommit(false)
	ctx := context.Background()
	_, err := db.Execute(ctx, `DROP SCHEMA IF EXISTS kwild_internal CASCADE`)
	require.NoError(t, err)
}

func rmDatasets(t *testing.T, db *pg.DB) {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx)
	require.NoError(t, err)

	defer tx.Rollback(ctx)

	_, err = tx.Execute(ctx, `DO $$
	DECLARE
		sn text;
	BEGIN
		FOR sn IN SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'ds_%'
		LOOP
			EXECUTE 'DROP SCHEMA ' || quote_ident(sn) || ' CASCADE';
		END LOOP;
	END $$;`)
	require.NoError(t, err)

	tx.Execute(ctx, `TRUNCATE kwild_internal.kwil_schemas CASCADE`)
	tx.Execute(ctx, `TRUNCATE kwild_internal.procedures CASCADE`)
	tx.Commit(ctx) // doesn't matter if err, just means tables didn't exist
}

// setup sets up the global context and registry for the tests
func setup(t *testing.T) (global *execution.GlobalContext, db *pg.DB) {
	ctx := context.Background()

	cfg := &pg.DBConfig{
		PoolConfig: pg.PoolConfig{
			ConnConfig: pg.ConnConfig{
				Host:   "127.0.0.1",
				Port:   "5432",
				User:   "kwild",
				Pass:   "kwild", // would be ignored if pg_hba.conf set with trust
				DBName: "kwil_test_db",
			},
			MaxConns: 11,
		},
		SchemaFilter: func(s string) bool {
			return strings.Contains(s, pg.DefaultSchemaFilterPrefix)
		},
	}
	var err error
	db, err = pg.NewDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rmDatasets(t, db)
	t.Cleanup(func() {
		cleanup(t, db)
	})

	tx, err := db.BeginTx(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = execution.InitializeEngine(ctx, tx)
	require.NoError(t, err)

	global, err = execution.NewGlobalContext(ctx, tx, map[string]actions.Initializer{
		"math": (&mathInitializer{}).initialize,
	}, &common.Service{
		Logger:           log.NewNoOp().Sugar(),
		ExtensionConfigs: map[string]map[string]string{},
	}, mockFS{})
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	return global, db
}

type mockFS struct{}

func (mf mockFS) ReadFile(name string) ([]byte, error) {
	return nil, nil
}
func (mf mockFS) WriteFile(name string, data []byte) error { return nil }

// mocks a namespace initializer
type mathInitializer struct {
	vals map[string]string
}

func (m *mathInitializer) initialize(_ *actions.DeploymentContext, _ *common.Service, mp map[string]string) (actions.Instance, error) {
	m.vals = mp

	_, ok := m.vals["fail"]
	if ok {
		return nil, fmt.Errorf("mock extension failed to initialize")
	}

	return &mathExt{}, nil
}

type mathExt struct{}

var _ actions.Instance = &mathExt{}

func (m *mathExt) Call(caller *actions.ProcedureContext, _ *common.App, method string, inputs []any) ([]any, bool, error) {
	if method != "add" {
		return nil, false, fmt.Errorf("unknown method: %s", method)
	}

	if len(inputs) != 2 {
		return nil, false, fmt.Errorf("expected 2 inputs, got %d", len(inputs))
	}

	// The extension needs to tolerate any compatible input type.

	a, err := conv.Int(inputs[0])
	if err != nil {
		return nil, false, fmt.Errorf("expected int64, got %T (%w)", inputs[0], err)
	}

	b, err := conv.Int(inputs[1])
	if err != nil {
		return nil, false, fmt.Errorf("expected int64, got %T (%w)", inputs[1], err)
	}

	return []any{a + b}, false, nil
}
