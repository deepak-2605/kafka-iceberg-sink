// Package buffer holds one in-progress batch per assigned partition. A batch
// records its start as the position fetching began from (not the first
// record's offset, since offsets can have gaps) and its end as the last
// record's offset + 1.
package buffer
