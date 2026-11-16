// Package failpoint crashes the process at named points when an environment
// variable asks for it, e.g. KIS_FAILPOINT=after-write (failpoint A) or
// KIS_FAILPOINT=after-iceberg-commit (failpoint B). Week 2.
package failpoint
