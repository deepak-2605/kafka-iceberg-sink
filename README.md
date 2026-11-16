# kafka-iceberg-sink

A Go service that reads events from Kafka and writes them to an Apache Iceberg
table exactly once, across replicas, crashes and rebalances. Offsets are stored
in the Iceberg snapshot summary and committed atomically with the data files;
a pinned compare-and-swap on `main` fences zombie writers.

Status: week 1 scaffold. The harness (load generator, verifier) and the
iceberg-go checks work; the sink itself is the week-1 build task.

## Quick start

Needs Go 1.25+ and Docker.

```bash
make up            # Kafka :9092, S3 (SeaweedFS) :8333, Iceberg REST catalog :8181
make test          # unit tests + iceberg-go checks on SQLite (no Docker)
make check-rest    # the same checks against the REST catalog
make loadgen RECORDS=1000000
make verify        # reads sink.events through DuckDB; exits 1 on any duplicate or loss
```

`make down` keeps data; `make down-clean` wipes it (also run `make reset-truth`).

## Week 1 findings (iceberg-go v0.7.0)

| Check | Result |
| --- | --- |
| Pinned commit (`Transaction.AssertRefSnapshotID`) rejects a stale base | Yes, on SQL and REST, with retries 0 and 4 |
| Library retries | Default is 0 (`commit.retry.num-retries`). With retries on and **no** pin, a stale commit is replayed onto the newer snapshot and succeeds: partition 1's offset rolled back from 9 to 5. With the pin, the retry loop keeps the original base and fails. The committer must always pin. |
| Snapshot summary properties round-trip; ancestor walk past a foreign snapshot | Yes |
| Write Parquet once, register the same path after a lost race | Yes, via `AddFiles`. Re-adding an already committed file is refused. |
| Catalog survives a restart | Yes (REST fixture on SQLite in a volume) |
| SQL catalog conflict error | Returns a plain `requirement failed` error, not `table.ErrCommitFailed`, so the library's retry loop never runs on it. Candidate upstream issue. |

Two environment notes:

- MinIO no longer publishes container images, so the stack uses SeaweedFS.
- AWS SDK v2 (2.30+) uploads with aws-chunked encoding and trailing
  checksums, which SeaweedFS stores as file content. The catalog sets
  `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED`; Go clients set
  `s3.compat-mode=true`.

## Layout

```
cmd/sink/             the sink (week 1 task)
cmd/loadgen/          idempotent producer, gap-free seq per partition, ground-truth file
cmd/verify/           DuckDB-based correctness checker
internal/tableschema/ Iceberg + Arrow schema with field IDs
internal/writer/      one Parquet file per partition per batch
internal/committer/   offset map, ancestor walk; week-1 checks in icebergchecks_test.go
internal/truth/       ground-truth file format
internal/{consumer,buffer,failpoint,metrics}/  stubs with their contracts
deploy/compose/       local stack
```

## Changelog

- 2026-11-16: scaffold, Compose stack, loadgen, verifier, iceberg-go checks pass on SQL and REST.
