// Command sink consumes Kafka and commits to Iceberg exactly once.
//
// Week 1 (16 to 22 Nov): single process, no crash handling yet.
//
//  1. Load the REST catalog and create sink.events if it does not exist
//     (schema: internal/tableschema).
//  2. Consume the topic with franz-go. Decode each JSON value into a
//     writer.Record, buffering per partition (internal/buffer).
//  3. Every 30 s, write one Parquet file per partition (internal/writer).
//  4. Commit all files in one transaction: AssertRefSnapshotID(main),
//     AddFiles(paths, {sink.offsets.<topic>.<group>: map}), Commit
//     (internal/committer). Use committer.ReadOffsets for the base map.
//
// Done when: `make loadgen RECORDS=1000000` then `make verify` prints PASS.
//
// Week 2 adds recovery on startup, the fencing check, the retry loop and
// failpoints A and B.
package main

import (
	"flag"
	"log"
)

func main() {
	_ = flag.String("brokers", "localhost:9092", "comma-separated seed brokers")
	_ = flag.String("topic", "events", "topic to consume")
	_ = flag.String("group", "sink", "consumer group")
	_ = flag.String("catalog", "http://localhost:8181", "Iceberg REST catalog endpoint")
	_ = flag.String("s3-endpoint", "http://localhost:8333", "S3 endpoint")
	_ = flag.String("table", "sink.events", "namespace.table to write")
	_ = flag.Duration("commit-interval", 30e9, "time between commits")
	flag.Parse()

	log.Fatal("sink: not implemented yet; this is the week-1 build task")
}
