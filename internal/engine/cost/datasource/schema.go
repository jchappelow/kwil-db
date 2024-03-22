package datasource

import "github.com/kwilteam/kwil-db/internal/engine/cost/datatypes"

// SchemaSource is an interface that provides the access to schema.
// It's used to get the schema of a table. It doesn't have the ability to
// scan the underlying data, which DataSource has.
type SchemaSource interface {
	// Schema returns the schema for the underlying data source
	Schema() *datatypes.Schema
}

// SchemaSourceToDataSource converts a SchemaSource to a DataSource.
func SchemaSourceToDataSource(s SchemaSource) DataSource {
	return nil
}
