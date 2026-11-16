// Package consumer wraps the franz-go client: partition assignment and revoke
// callbacks, seeking each assigned partition to the offset stored in the
// table (earliest when absent), and committing offsets to Kafka after a
// successful Iceberg commit, for lag dashboards only.
//
// Use the cooperative-sticky balancer. Never recover from Kafka's committed
// offsets.
package consumer
