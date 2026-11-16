// Package writer turns one partition's batch into one Parquet data file.
//
// Files are written once and never rewritten. The committer registers the same
// path on every commit attempt (plan step 6), and a file whose partition fails
// the fencing check is simply left out of the commit and becomes an orphan.
package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceio "github.com/apache/iceberg-go/io"

	"github.com/deepak-2605/kafka-iceberg-sink/internal/tableschema"
)

// Record is one row of the table.
type Record struct {
	Partition  int32
	Offset     int64
	Seq        int64
	ProducedAt time.Time
	Payload    string
}

// WriteFile writes recs to path as a single Parquet file. All records must
// come from the same Kafka partition.
func WriteFile(ctx context.Context, fio iceio.WriteFileIO, path string, recs []Record) error {
	if len(recs) == 0 {
		return fmt.Errorf("writer: refusing to write empty file %s", path)
	}
	for _, r := range recs[1:] {
		if r.Partition != recs[0].Partition {
			return fmt.Errorf("writer: file %s mixes partitions %d and %d", path, recs[0].Partition, r.Partition)
		}
	}

	sc, err := tableschema.Arrow()
	if err != nil {
		return err
	}

	rec := build(sc, recs)
	defer rec.Release()

	tbl := array.NewTableFromRecords(sc, []arrow.Record{rec})
	defer tbl.Release()

	out, err := fio.Create(path)
	if err != nil {
		return fmt.Errorf("writer: create %s: %w", path, err)
	}
	if err := pqarrow.WriteTable(tbl, out, tbl.NumRows(), nil, pqarrow.DefaultWriterProps()); err != nil {
		return fmt.Errorf("writer: write %s: %w", path, err)
	}

	return nil
}

func build(sc *arrow.Schema, recs []Record) arrow.Record {
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()

	part := b.Field(0).(*array.Int32Builder)
	off := b.Field(1).(*array.Int64Builder)
	seq := b.Field(2).(*array.Int64Builder)
	ts := b.Field(3).(*array.TimestampBuilder)
	payload := b.Field(4).(*array.StringBuilder)

	for _, r := range recs {
		part.Append(r.Partition)
		off.Append(r.Offset)
		seq.Append(r.Seq)
		ts.Append(arrow.Timestamp(r.ProducedAt.UnixMicro()))
		payload.Append(r.Payload)
	}

	return b.NewRecord()
}
