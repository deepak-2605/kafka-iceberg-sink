// Command loadgen produces records shaped
//
//	{"partition": p, "seq": s, "produced_at": "...", "payload": "..."}
//
// where seq starts at 0 and rises by one per partition with no gaps. It writes
// the number of acknowledged records per partition to a ground-truth file,
// which the verifier compares against the table.
//
// Running it again continues from the counts already in the truth file, so a
// test can produce in several bursts.
//
// The producer is idempotent (franz-go's default; never disable it), so
// producer retries cannot put duplicate seqs into Kafka and be blamed on the
// sink.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/deepak-2605/kafka-iceberg-sink/internal/truth"
)

type event struct {
	Partition  int32     `json:"partition"`
	Seq        int64     `json:"seq"`
	ProducedAt time.Time `json:"produced_at"`
	Payload    string    `json:"payload"`
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated seed brokers")
	topic := flag.String("topic", "events", "topic to produce to")
	partitions := flag.Int("partitions", 12, "number of partitions in the topic")
	records := flag.Int64("records", 1_000_000, "records to produce in this run, spread evenly across partitions")
	rate := flag.Int("rate", 0, "target records per second; 0 means as fast as possible")
	payloadBytes := flag.Int("payload-bytes", 200, "size of the payload string")
	out := flag.String("truth", "ground-truth.json", "ground-truth file to continue from and write to")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gt, err := truth.Load(*out)
	if err != nil {
		log.Fatalf("load truth: %v", err)
	}
	if gt.Topic != "" && gt.Topic != *topic {
		log.Fatalf("truth file is for topic %q, not %q", gt.Topic, *topic)
	}
	gt.Topic = *topic

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	defer cl.Close()

	// next[p] is the seq to assign next; acked[p] counts acknowledged records.
	// With an idempotent producer, acks arrive in order per partition, so the
	// acked count is exactly the prefix of seqs that made it into Kafka.
	next := make([]int64, *partitions)
	acked := make([]atomic.Int64, *partitions)
	for p := range next {
		next[p] = gt.Acked[int32(p)]
		acked[p].Store(next[p])
	}

	payload := strings.Repeat("x", *payloadBytes)
	var (
		firstErr error
		errOnce  sync.Once
		failed   atomic.Bool
		wg       sync.WaitGroup
	)

	start := time.Now()
	var sent int64
	for sent = 0; sent < *records && ctx.Err() == nil && !failed.Load(); sent++ {
		p := int32(sent % int64(*partitions))
		ev := event{Partition: p, Seq: next[p], ProducedAt: time.Now().UTC(), Payload: payload}
		next[p]++

		val, _ := json.Marshal(ev)
		wg.Add(1)
		cl.Produce(ctx, &kgo.Record{Partition: p, Value: val}, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				failed.Store(true)
				errOnce.Do(func() { firstErr = err })
				return
			}
			acked[r.Partition].Add(1)
		})

		if *rate > 0 && sent%100 == 0 {
			want := time.Duration(float64(sent) / float64(*rate) * float64(time.Second))
			if d := want - time.Since(start); d > 0 {
				time.Sleep(d)
			}
		}
		if sent > 0 && sent%100_000 == 0 {
			log.Printf("produced %d records", sent)
		}
	}

	if err := cl.Flush(context.Background()); err != nil {
		log.Printf("flush: %v", err)
	}
	wg.Wait()

	for p := range acked {
		gt.Acked[int32(p)] = acked[p].Load()
	}
	gt.Complete = firstErr == nil && ctx.Err() == nil
	if err := truth.Save(*out, gt); err != nil {
		log.Fatalf("save truth: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("produced %d records in %s (%.0f records/s); truth total %d in %s\n",
		sent, elapsed.Round(time.Millisecond), float64(sent)/elapsed.Seconds(), gt.Total(), *out)
	if firstErr != nil {
		log.Fatalf("stopped on produce error: %v", firstErr)
	}
}
