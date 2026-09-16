# ADR 007: Publish one approved immutable run per curated month

- **Status:** Accepted
- **Date:** 2026-09-16

## Context

Daily staging is a validation boundary, not an analytics source. Lambda staging keys
include an immutable `input_id`, so retries and versioned reprocessing can leave several
attempts for one source date. Querying that prefix directly could double-count telemetry
and would couple analysts to operational completion markers.

The curated monthly contract keeps immutable compaction runs at:

```text
curated/metropt3/year=YYYY/month=MM/run_id=<approved-run-id>/part-00000.snappy.parquet
```

The `run_id` identifies a complete physical publication. It is not an analytical
partition and must not require analysts to choose an internal pipeline identity.

## Decision

Athena queries curated monthly Parquet only. The Glue table has exactly two partition
columns, `year` and `month`; `run_id` is not exposed as a query partition. Partition
projection is disabled, and `MSCK REPAIR TABLE` must not be used.

For each year/month, one explicitly registered Glue partition points to the exact
approved immutable run directory. A future compactor may publish that partition only
after its curated object, reconciliation, checksum verification, and completion manifest
succeed. Replacing a monthly run creates and verifies a new immutable run first, then
updates the existing year/month partition location. It never exposes an incomplete run
or appends to Parquet in place.

Phase 5 defines the empty database, table, and workgroup contracts. It creates no Glue
partitions because no curated output exists. Phase 6 will implement monthly compaction,
completion publication, and explicit partition registration.

## Consequences

- Queries see one approved run per month without handling attempt identities.
- Daily staging, quarantine, raw data, control markers, and manifests are not cataloged.
- Automatic discovery cannot accidentally expose obsolete or incomplete runs.
- The table remains empty until Phase 6 publishes an approved curated partition.
- Reprocessing changes an operational partition location without changing the table
  schema or its `year` and `month` query contract.
