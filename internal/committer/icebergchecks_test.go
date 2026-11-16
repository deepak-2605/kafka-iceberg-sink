package committer

// Week-1 checks: does iceberg-go do what the exactly-once protocol needs?
//
// Every check runs against two catalogs:
//
//	sql   iceberg-go's SQL catalog on SQLite with a local warehouse. Always runs.
//	rest  the Compose stack (REST catalog on :8181, S3 on :8333). Runs only
//	      with KIS_REST=1, e.g. `make check-rest`.
//
// The REST run is the one that matters: a 409 from the REST catalog becomes
// table.ErrCommitFailed, which is the only error iceberg-go's retry loop acts
// on. The SQL catalog never reaches that loop (see isConflict).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/rest"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/uptrace/bun/driver/sqliteshim"

	"github.com/deepak-2605/kafka-iceberg-sink/internal/tableschema"
	"github.com/deepak-2605/kafka-iceberg-sink/internal/writer"
)

const (
	topic = "events"
	group = "sink"
)

type env struct {
	t     *testing.T
	ctx   context.Context
	cat   catalog.Catalog
	ident table.Identifier
}

type factory func(t *testing.T, retries int) *env

// catalogs returns the catalogs every check runs against.
func catalogs() map[string]factory {
	m := map[string]factory{"sql": newSQLEnv}
	if os.Getenv("KIS_REST") == "1" {
		m["rest"] = newRESTEnv
	}
	return m
}

func newSQLEnv(t *testing.T, retries int) *env {
	t.Helper()
	dir := t.TempDir()
	cat, err := catalog.Load(context.Background(), "test", iceberg.Properties{
		"type":                "sql",
		"uri":                 "file://" + filepath.Join(dir, "catalog.db"),
		sqlcat.DriverKey:      sqliteshim.ShimName,
		sqlcat.DialectKey:     string(sqlcat.SQLite),
		"warehouse":           "file://" + filepath.Join(dir, "warehouse"),
		"init_catalog_tables": "true",
	})
	if err != nil {
		t.Fatalf("load sql catalog: %v", err)
	}
	return setup(t, cat, retries)
}

func newRESTEnv(t *testing.T, retries int) *env {
	t.Helper()
	cat, err := catalog.Load(context.Background(), "test", iceberg.Properties{
		"type":                  "rest",
		"uri":                   envOr("KIS_CATALOG_URI", "http://localhost:8181"),
		iceio.S3EndpointURL:     envOr("KIS_S3_ENDPOINT", "http://localhost:8333"),
		iceio.S3AccessKeyID:     "admin",
		iceio.S3SecretAccessKey: "password",
		iceio.S3Region:          "us-east-1",
		"s3.path-style-access":  "true",
		iceio.S3CompatMode:      "true", // checksums only when required; see docker-compose.yml
	})
	if err != nil {
		t.Fatalf("load rest catalog: %v", err)
	}
	return setup(t, cat, retries)
}

// setup creates a fresh namespace and table, unique per test, so runs against
// the long-lived Compose catalog never collide.
func setup(t *testing.T, cat catalog.Catalog, retries int) *env {
	t.Helper()
	ctx := context.Background()
	ns := "check_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if err := cat.CreateNamespace(ctx, table.Identifier{ns}, nil); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	ident := table.Identifier{ns, "events"}
	_, err := cat.CreateTable(ctx, ident, tableschema.Iceberg(), catalog.WithProperties(iceberg.Properties{
		table.CommitNumRetriesKey:     strconv.Itoa(retries),
		table.CommitMinRetryWaitMsKey: "10",
		table.CommitMaxRetryWaitMsKey: "20",
	}))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	if os.Getenv("KIS_KEEP") == "1" {
		t.Logf("keeping table %s.events", ns)
	} else {
		t.Cleanup(func() {
			_ = cat.DropTable(ctx, ident)
			_ = cat.DropNamespace(ctx, table.Identifier{ns})
		})
	}
	return &env{t: t, ctx: ctx, cat: cat, ident: ident}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// forEach runs fn once per catalog and retry setting.
func forEach(t *testing.T, retries []int, fn func(t *testing.T, e *env)) {
	for name, f := range catalogs() {
		for _, r := range retries {
			t.Run(fmt.Sprintf("%s/retries=%d", name, r), func(t *testing.T) { fn(t, f(t, r)) })
		}
	}
}

// load returns a fresh handle on the table, like a replica would hold.
func (e *env) load() *table.Table {
	e.t.Helper()
	tbl, err := e.cat.LoadTable(e.ctx, e.ident)
	if err != nil {
		e.t.Fatalf("load table: %v", err)
	}
	return tbl
}

// writeFile writes one Parquet file for partition p with n records.
func (e *env) writeFile(tbl *table.Table, name string, p int32, n int) string {
	e.t.Helper()
	fs, err := tbl.FS(e.ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	recs := make([]writer.Record, n)
	for i := range recs {
		recs[i] = writer.Record{Partition: p, Offset: int64(i), Seq: int64(i), ProducedAt: time.Unix(0, 0), Payload: "x"}
	}
	path := tbl.Location() + "/data/" + name + ".parquet"
	if err := writer.WriteFile(e.ctx, fs.(iceio.WriteFileIO), path, recs); err != nil {
		e.t.Fatal(err)
	}
	return path
}

// commit registers files and the offset map in one snapshot. pin adds the
// explicit compare-and-swap on main that the protocol relies on.
func (e *env) commit(tbl *table.Table, files []string, offsets OffsetMap, pin bool) error {
	e.t.Helper()
	tx := tbl.NewTransaction()
	if pin {
		if err := tx.AssertRefSnapshotID(table.MainBranch); err != nil {
			return err
		}
	}
	var props iceberg.Properties
	if offsets != nil {
		v, err := offsets.Encode()
		if err != nil {
			return err
		}
		props = iceberg.Properties{PropertyKey(topic, group): v}
	}
	if err := tx.AddFiles(e.ctx, files, props, false); err != nil {
		return err
	}
	_, err := tx.Commit(e.ctx)
	return err
}

func (e *env) offsets() OffsetMap {
	e.t.Helper()
	m, _, err := ReadOffsets(e.load(), PropertyKey(topic, group))
	if err != nil {
		e.t.Fatal(err)
	}
	return m
}

// isConflict reports whether err is a lost compare-and-swap. The REST catalog
// maps HTTP 409 to table.ErrCommitFailed. The SQL catalog in iceberg-go v0.7.0
// returns an unwrapped "requirement failed" error instead, so the library's
// retry loop never runs against it (candidate upstream issue).
func isConflict(err error) bool {
	return err != nil && (errors.Is(err, table.ErrCommitFailed) || strings.Contains(err.Error(), "requirement failed"))
}

func totalRecords(tbl *table.Table) string {
	return tbl.CurrentSnapshot().Summary.Properties["total-records"]
}

// Check 1: a commit built on a stale snapshot is rejected when it pins main,
// whatever the library's retry setting.
func TestCheck1_PinnedCommitRejectsStaleBase(t *testing.T) {
	forEach(t, []int{0, 4}, func(t *testing.T, e *env) {
		seed := e.load()
		if err := e.commit(seed, []string{e.writeFile(seed, "seed", 0, 5)}, OffsetMap{0: 5}, true); err != nil {
			t.Fatalf("seed commit: %v", err)
		}

		stale := e.load() // replica A reads S1
		fresh := e.load() // replica B reads S1 too
		if err := e.commit(fresh, []string{e.writeFile(fresh, "b", 1, 3)}, OffsetMap{0: 5, 1: 3}, true); err != nil {
			t.Fatalf("fresh commit: %v", err)
		}

		err := e.commit(stale, []string{e.writeFile(stale, "a", 0, 4)}, OffsetMap{0: 9}, true)
		if !isConflict(err) {
			t.Fatalf("stale pinned commit: want a conflict, got %v", err)
		}
		t.Logf("rejected as: %v", err)
		if got := e.offsets(); got[0] != 5 || got[1] != 3 {
			t.Fatalf("offset map changed by a rejected commit: %v", got)
		}
	})
}

// Check 2: what happens WITHOUT the pin. With retries on, does the library
// replay a stale offset map onto the newer snapshot? This test records the
// behaviour; it is why the committer always pins. Against the SQL catalog the
// retry loop never runs (see isConflict), so the replay case is only really
// exercised by the REST integration test.
func TestCheck2_UnpinnedStaleCommit(t *testing.T) {
	forEach(t, []int{0, 4}, func(t *testing.T, e *env) {
		seed := e.load()
		if err := e.commit(seed, []string{e.writeFile(seed, "seed", 0, 5)}, OffsetMap{0: 5, 1: 5}, false); err != nil {
			t.Fatalf("seed commit: %v", err)
		}

		stale := e.load()
		fresh := e.load()
		// B advances partition 1 from 5 to 9.
		if err := e.commit(fresh, []string{e.writeFile(fresh, "b", 1, 4)}, OffsetMap{0: 5, 1: 9}, false); err != nil {
			t.Fatalf("fresh commit: %v", err)
		}
		// A, still on S1, advances partition 0 and carries the stale 1 -> 5.
		err := e.commit(stale, []string{e.writeFile(stale, "a", 0, 3)}, OffsetMap{0: 8, 1: 5}, false)

		got := e.offsets()
		switch {
		case isConflict(err):
			t.Logf("unpinned stale commit rejected (%v); map stays %v", err, got)
		case err == nil:
			t.Logf("UNSAFE: unpinned stale commit replayed and succeeded; map is now %v (partition 1 rolled back from 9 to %d)", got, got[1])
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// Check 3: snapshot properties round-trip, and the ancestor walk finds the map
// when another writer's snapshot sits on top.
func TestCheck3_SnapshotPropsAndAncestorWalk(t *testing.T) {
	forEach(t, []int{0}, check3)
}

func check3(t *testing.T, e *env) {
	tbl := e.load()

	if m, base, err := ReadOffsets(tbl, PropertyKey(topic, group)); err != nil || len(m) != 0 || base != nil {
		t.Fatalf("empty table: want empty map and nil base, got %v %v %v", m, base, err)
	}

	if err := e.commit(tbl, []string{e.writeFile(tbl, "s1", 0, 5)}, OffsetMap{0: 5, 3: 7}, true); err != nil {
		t.Fatal(err)
	}
	if got := e.offsets(); got[0] != 5 || got[3] != 7 {
		t.Fatalf("round trip: got %v", got)
	}

	// A foreign writer appends with no offset property.
	other := e.load()
	if err := e.commit(other, []string{e.writeFile(other, "foreign", 2, 1)}, nil, false); err != nil {
		t.Fatal(err)
	}
	tbl = e.load()
	if _, ok := tbl.CurrentSnapshot().Summary.Properties[PropertyKey(topic, group)]; ok {
		t.Fatal("foreign snapshot unexpectedly carries the offset property")
	}
	m, base, err := ReadOffsets(tbl, PropertyKey(topic, group))
	if err != nil {
		t.Fatal(err)
	}
	if m[0] != 5 || m[3] != 7 {
		t.Fatalf("ancestor walk: got %v", m)
	}
	if base == nil || *base != tbl.CurrentSnapshot().SnapshotID {
		t.Fatalf("base must be the current snapshot, got %v", base)
	}
}

// Check 4: a file written once can be registered again after a lost race,
// and registering an already committed file is refused.
func TestCheck4_SameFilesAfterLostRace(t *testing.T) {
	forEach(t, []int{0}, check4)
}

func check4(t *testing.T, e *env) {
	seed := e.load()
	if err := e.commit(seed, []string{e.writeFile(seed, "seed", 0, 5)}, OffsetMap{0: 5}, true); err != nil {
		t.Fatal(err)
	}

	stale := e.load()
	file := e.writeFile(stale, "a-p1", 1, 4) // written exactly once

	fresh := e.load()
	if err := e.commit(fresh, []string{e.writeFile(fresh, "b-p2", 2, 2)}, OffsetMap{0: 5, 2: 2}, true); err != nil {
		t.Fatal(err)
	}

	if err := e.commit(stale, []string{file}, OffsetMap{0: 5, 1: 4}, true); !isConflict(err) {
		t.Fatalf("first attempt: want a conflict, got %v", err)
	}

	// Retry the protocol way: reload, re-read the map, same file.
	retry := e.load()
	m, _, err := ReadOffsets(retry, PropertyKey(topic, group))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.commit(retry, []string{file}, m.Advance(OffsetMap{1: 4}), true); err != nil {
		t.Fatalf("retry with the same file: %v", err)
	}

	tbl := e.load()
	if got := totalRecords(tbl); got != "11" {
		t.Fatalf("total-records: want 11 (5+2+4), got %s", got)
	}
	if got := e.offsets(); got[0] != 5 || got[1] != 4 || got[2] != 2 {
		t.Fatalf("final map: got %v", got)
	}

	// Safety net: adding a file that is already in the table is refused.
	if err := e.commit(tbl, []string{file}, nil, true); err == nil {
		t.Fatal("re-adding a committed file should fail")
	}
}
