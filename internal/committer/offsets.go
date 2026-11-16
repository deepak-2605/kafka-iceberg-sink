// Package committer owns the exactly-once protocol: the offset map stored in
// the snapshot summary, the fencing check, and the commit retry loop.
package committer

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/apache/iceberg-go/table"
)

// OffsetMap maps a Kafka partition to the next offset to consume.
type OffsetMap map[int32]int64

// PropertyKey is the snapshot summary key holding the offset map for one
// topic and consumer group, so two sinks can share a table.
func PropertyKey(topic, group string) string {
	return "sink.offsets." + topic + "." + group
}

// Encode serialises the map as a JSON object with string keys.
func (m OffsetMap) Encode() (string, error) {
	out := make(map[string]int64, len(m))
	for p, o := range m {
		out[strconv.Itoa(int(p))] = o
	}
	b, err := json.Marshal(out)
	return string(b), err
}

// Decode parses a value written by Encode.
func Decode(s string) (OffsetMap, error) {
	var raw map[string]int64
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil, fmt.Errorf("committer: bad offset map %q: %w", s, err)
	}
	m := make(OffsetMap, len(raw))
	for k, v := range raw {
		p, err := strconv.Atoi(k)
		if err != nil {
			return nil, fmt.Errorf("committer: bad partition key %q: %w", k, err)
		}
		m[int32(p)] = v
	}
	return m, nil
}

// Advance returns a copy of m with the given partitions moved to new offsets.
func (m OffsetMap) Advance(next OffsetMap) OffsetMap {
	out := make(OffsetMap, len(m)+len(next))
	for p, o := range m {
		out[p] = o
	}
	for p, o := range next {
		out[p] = o
	}
	return out
}

// ReadOffsets finds the offset map for key, starting at the table's current
// snapshot and walking back through parents until a snapshot carries the
// property. Snapshots written by other writers (compaction, a Spark job) do
// not carry it, so the current snapshot alone is not enough.
//
// It returns an empty map when no snapshot has the property, which means
// every partition starts from the earliest offset. It also returns the ID of
// the current snapshot, which is the base the next commit must assert.
func ReadOffsets(tbl *table.Table, key string) (OffsetMap, *int64, error) {
	cur := tbl.CurrentSnapshot()
	if cur == nil {
		return OffsetMap{}, nil, nil
	}
	base := cur.SnapshotID

	for s := cur; s != nil; {
		if s.Summary != nil {
			if v, ok := s.Summary.Properties[key]; ok {
				m, err := Decode(v)
				return m, &base, err
			}
		}
		if s.ParentSnapshotID == nil {
			break
		}
		parent := tbl.SnapshotByID(*s.ParentSnapshotID)
		if parent == nil {
			// The ancestor was expired. Refuse to guess: starting from
			// earliest here would replay the whole topic into the table.
			return nil, &base, fmt.Errorf("committer: snapshot %d expired before an offset map was found", *s.ParentSnapshotID)
		}
		s = parent
	}

	return OffsetMap{}, &base, nil
}
