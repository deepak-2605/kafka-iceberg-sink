// Package tableschema defines the one fixed schema the sink writes in v0.1.0.
// The Iceberg schema and the Arrow schema used for Parquet files come from the
// same definition, so field IDs always line up.
package tableschema

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Field IDs. Never renumber these: data files already written carry them.
const (
	FieldPartition  = 1
	FieldOffset     = 2
	FieldSeq        = 3
	FieldProducedAt = 4
	FieldPayload    = 5
)

// Iceberg returns the table schema.
//
//	partition    int    Kafka partition the record came from
//	offset       long   Kafka offset of the record
//	seq          long   load generator sequence number, gap-free per partition
//	produced_at  timestamptz
//	payload      string
func Iceberg() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: FieldPartition, Name: "partition", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: FieldOffset, Name: "offset", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: FieldSeq, Name: "seq", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: FieldProducedAt, Name: "produced_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: FieldPayload, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

// Arrow returns the Arrow schema with PARQUET:field_id metadata on every field,
// which is what lets AddFiles map Parquet columns to Iceberg fields.
func Arrow() (*arrow.Schema, error) {
	return table.SchemaToArrowSchema(Iceberg(), nil, true, false)
}
