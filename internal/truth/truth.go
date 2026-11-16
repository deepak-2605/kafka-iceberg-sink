// Package truth is the ground-truth file shared by the load generator and the
// verifier: for each partition, how many records were acknowledged by Kafka.
// Because seq starts at 0 and has no gaps, that count is also max(seq) + 1.
package truth

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
)

// File is the on-disk format.
type File struct {
	Topic string `json:"topic"`
	// Acked maps partition to the number of records acknowledged.
	Acked map[int32]int64 `json:"acked"`
	// Complete is false when the generator stopped on an error. The counts
	// are still exact, but the run should not be trusted as a benchmark.
	Complete bool `json:"complete"`
}

// Load reads path. A missing file returns an empty File and no error.
func Load(path string) (File, error) {
	f := File{Acked: map[int32]int64{}}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return f, nil
	}
	if err != nil {
		return f, err
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return f, err
	}
	if f.Acked == nil {
		f.Acked = map[int32]int64{}
	}
	return f, nil
}

// Save writes f to path atomically.
func Save(path string, f File) error {
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Total returns the sum of all partitions.
func (f File) Total() int64 {
	var n int64
	for _, v := range f.Acked {
		n += v
	}
	return n
}
