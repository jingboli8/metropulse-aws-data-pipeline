# ADR 009: Static-source scheduled operations

- **Status:** Accepted
- **Date:** 2026-09-18

## Context

MetroPT-3 is a closed historical source ending at 2020-09-01 03:59:50. New source data
does not arrive each month. Scheduling recurring ingestion or compaction would imply a
live feed that does not exist. Phase 6 also requires one explicit immutable
`MonthlySelection`; listing S3 cannot establish which validation attempt is approved.

## Decision

Monthly compaction remains on demand. An operator or deployment process reviews exact
daily completion-marker references, validates them, publishes the canonical selection at
its deterministic run key, and invokes the compaction Lambda with that exact pinned
reference. Every invocation receives a unique `owner_token` derived from its Lambda
request ID. The deterministic `run_id` identifies processing and never substitutes for
claim ownership.

EventBridge Scheduler runs only the recurring full-checksum integrity audit. Its weekly
Monday 06:00 UTC schedule is disabled by default and may be enabled only after all
curated partitions and an exact immutable audit inventory exist. Scheduler supplies the
pinned inventory reference and its own context attributes. Neither workflow lists a
bucket to infer approval or current state.

The audit reports known missing source dates, the partial terminal month, and timestamp
gaps as source-quality observations. Broken evidence, count reconciliation, or current
Glue location causes operational failure. A heartbeat alarm is omitted: CloudWatch caps
the relevant alarm evaluation window at seven days, so it cannot safely express a
weekly cadence plus delivery and metric-ingestion grace. Ordinary failure alarms treat
missing data as non-breaching.

## Consequences

Historical backfill is explicit and reviewable. The only recurring operation has useful
integrity semantics even though the source is static. Enabling the schedule requires a
reviewed Terraform input change. S3 claims serialize normal publication, but S3 and Glue
still have no cross-service transaction and absolute fencing of a paused former
publisher remains impossible.
