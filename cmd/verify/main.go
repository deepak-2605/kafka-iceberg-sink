// Command verify checks the Iceberg table against the load generator's ground
// truth and exits non-zero on any duplicate or missing record.
//
// It reads the table through DuckDB's Iceberg extension against the same REST
// catalog the sink uses, so the sink's own code never checks itself.
//
// Per partition p with n acknowledged records it requires:
//
//	count(*) = count(distinct seq) = max(seq) + 1 = n
//
// and prints the duplicated and missing seqs when that fails.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/deepak-2605/kafka-iceberg-sink/internal/truth"
)

type stats struct {
	rows, distinct, maxSeq int64
}

func main() {
	truthPath := flag.String("truth", "ground-truth.json", "ground-truth file written by loadgen")
	catalogURI := flag.String("catalog", "http://localhost:8181", "Iceberg REST catalog endpoint")
	warehouse := flag.String("warehouse", "warehouse", "warehouse name to attach")
	s3Endpoint := flag.String("s3-endpoint", "localhost:8333", "S3 endpoint as host:port")
	s3Key := flag.String("s3-key", "admin", "S3 access key")
	s3Secret := flag.String("s3-secret", "password", "S3 secret key")
	tableName := flag.String("table", "sink.events", "namespace.table to verify")
	show := flag.Int("show", 10, "max duplicate and missing seqs to print per partition")
	flag.Parse()

	gt, err := truth.Load(*truthPath)
	if err != nil {
		log.Fatalf("load truth: %v", err)
	}
	if len(gt.Acked) == 0 {
		log.Fatalf("truth file %s is empty or missing", *truthPath)
	}
	if !gt.Complete {
		log.Printf("warning: %s is marked incomplete (the generator stopped early); counts are still exact", *truthPath)
	}

	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // secrets and ATTACH are per connection

	setup := []string{
		"INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs",
		fmt.Sprintf(`CREATE SECRET s3 (TYPE S3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s',
			URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')`, *s3Key, *s3Secret, *s3Endpoint),
		fmt.Sprintf(`ATTACH '%s' AS cat (TYPE ICEBERG, ENDPOINT '%s', AUTHORIZATION_TYPE 'none')`, *warehouse, *catalogURI),
	}
	for _, q := range setup {
		if _, err := db.ExecContext(ctx, q); err != nil {
			log.Fatalf("duckdb setup %q: %v", firstLine(q), err)
		}
	}

	tbl := "cat." + *tableName
	got := map[int32]stats{}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT partition, count(*), count(DISTINCT seq), max(seq) FROM %s GROUP BY partition`, tbl))
	if err != nil {
		log.Fatalf("query %s: %v", tbl, err)
	}
	for rows.Next() {
		var p int32
		var s stats
		if err := rows.Scan(&p, &s.rows, &s.distinct, &s.maxSeq); err != nil {
			log.Fatal(err)
		}
		got[p] = s
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	parts := map[int32]bool{}
	for p := range gt.Acked {
		parts[p] = true
	}
	for p := range got {
		parts[p] = true
	}
	order := make([]int32, 0, len(parts))
	for p := range parts {
		order = append(order, p)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	var totalWant, totalRows, totalDup, totalMissing, totalExtra int64
	bad := false
	fmt.Printf("%-9s %10s %10s %10s %10s %8s %8s %8s\n", "partition", "want", "rows", "distinct", "max+1", "dup", "missing", "extra")
	for _, p := range order {
		want := gt.Acked[p]
		s, ok := got[p]
		maxPlus1 := int64(0)
		if ok {
			maxPlus1 = s.maxSeq + 1
		}
		dup := s.rows - s.distinct
		extra := int64(0) // seqs at or beyond want: in the table but never acked
		if maxPlus1 > want {
			extra = countBeyond(ctx, db, tbl, p, want)
		}
		missing := want - (s.distinct - extra)

		totalWant += want
		totalRows += s.rows
		totalDup += dup
		totalMissing += missing
		totalExtra += extra

		mark := ""
		if dup != 0 || missing != 0 || extra != 0 {
			bad, mark = true, "  <- FAIL"
		}
		fmt.Printf("%-9d %10d %10d %10d %10d %8d %8d %8d%s\n", p, want, s.rows, s.distinct, maxPlus1, dup, missing, extra, mark)

		if dup != 0 {
			printDuplicates(ctx, db, tbl, p, *show)
		}
		if missing != 0 {
			printMissing(ctx, db, tbl, p, want, *show)
		}
	}

	fmt.Printf("\ntotal: want %d, rows %d, duplicates %d, missing %d, extra %d\n", totalWant, totalRows, totalDup, totalMissing, totalExtra)
	if bad {
		fmt.Println("FAIL")
		os.Exit(1)
	}
	fmt.Println("PASS: 0 duplicates, 0 missing")
}

func countBeyond(ctx context.Context, db *sql.DB, tbl string, p int32, want int64) int64 {
	var n int64
	q := fmt.Sprintf(`SELECT count(DISTINCT seq) FROM %s WHERE partition = ? AND seq >= ?`, tbl)
	if err := db.QueryRowContext(ctx, q, p, want).Scan(&n); err != nil {
		log.Fatal(err)
	}
	return n
}

func printDuplicates(ctx context.Context, db *sql.DB, tbl string, p int32, limit int) {
	q := fmt.Sprintf(`SELECT seq, count(*) FROM %s WHERE partition = ? GROUP BY seq HAVING count(*) > 1 ORDER BY seq LIMIT ?`, tbl)
	rows, err := db.QueryContext(ctx, q, p, limit)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var seq, c int64
		if err := rows.Scan(&seq, &c); err != nil {
			log.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%d(x%d)", seq, c))
	}
	fmt.Printf("          duplicated seqs: %s\n", strings.Join(out, " "))
}

// printMissing prints missing seqs as ranges, e.g. 120-379 4410.
func printMissing(ctx context.Context, db *sql.DB, tbl string, p int32, want int64, limit int) {
	q := fmt.Sprintf(`
		WITH m AS (
			SELECT range AS seq FROM range(?)
			EXCEPT SELECT seq FROM %s WHERE partition = ?
		), g AS (
			SELECT seq, seq - row_number() OVER (ORDER BY seq) AS grp FROM m
		)
		SELECT min(seq), max(seq) FROM g GROUP BY grp ORDER BY 1 LIMIT ?`, tbl)
	rows, err := db.QueryContext(ctx, q, want, p, limit)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var lo, hi int64
		if err := rows.Scan(&lo, &hi); err != nil {
			log.Fatal(err)
		}
		if lo == hi {
			out = append(out, fmt.Sprint(lo))
		} else {
			out = append(out, fmt.Sprintf("%d-%d", lo, hi))
		}
	}
	fmt.Printf("          missing seqs: %s\n", strings.Join(out, " "))
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
