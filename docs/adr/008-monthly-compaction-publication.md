# ADR 008: Monthly compaction publication

**Status:** Accepted

## Context

Daily Lambda staging keys are immutable attempts identified by `input_id`. Listing a
prefix cannot establish which attempt is approved. Glue exposes a month to Athena, but
S3 and Glue provide no cross-service transaction and Glue `UpdatePartition` has no
conditional location update or fencing token.

## Decision

An explicit `MonthlySelection` pins exactly one completed daily marker and staging object
per selected source date. The compactor reads only those exact keys; it never lists S3 to
choose an attempt. The selection records the expected raw-date inventory, known source
range, and terminal-month status. Missing dates are coverage findings and are never
imputed.

The run ID is the lowercase SHA-256 of canonical UTF-8 JSON under contract
`metropulse-monthly-compaction-run-v1`. It covers the sorted selected identities,
checksums, sizes and counts; schema, pipeline and manifest versions; compactor and output
contract versions; and every material PyArrow/Parquet writer option. It excludes caller
order, invocation time, request IDs, claim data, local paths, current Glue location, and
the output checksum.

Every run writes one immutable object:

`curated/metropt3/year=YYYY/month=MM/run_id=<run-id>/part-00000.snappy.parquet`

Selection and completion evidence use the same run-specific control prefix. Creation is
conditional. Existing content must match exactly and pass full Parquet verification;
otherwise processing fails without deletion or replacement. Completion is written only
after the output is reopened and its schema, Snappy codec, ordering, row groups,
timestamps, checksum, size, and row counts are verified.

The Glue partition is the analytics visibility switch. Values are exactly `[year,
month]`; its location ends in `/` and names one completed run. A replacement preserves
the prior location until the new output and completion evidence are valid. Publication
creates, no-ops at the same location, or performs a guarded replacement only when the
caller supplies the expected current location. A third location is a conflict.

A conditional S3 month-publication claim serializes normal operation. Ownership is
checked immediately before Glue mutation and again during completion handling. The
publisher never automatically takes over a stale claim; reconciliation or explicit
operator authorization is required. Explicit authorization pins the exact prior claim
ETag and replaces it conditionally; an intervening change remains a conflict. This claim
is not an S3/Glue transaction. Absolute
fencing against a paused former publisher is impossible with this design, so Phase 7
also applies bounded/reserved scheduler concurrency. Ambiguous Glue responses are read
back and accepted only at the expected old or fully completed requested location.

February 2020 may report 2020-02-29 missing, April may report 2020-04-26 missing, and
September is an explicit valid partial terminal month ending 2020-09-01. No case creates
synthetic observations or quarantine rows. `published.json` is an immutable historical
receipt; it does not claim that its run remains the current Glue location forever.

## Consequences

Retries before output, after output, after completion, and after Glue update remain
safe. A failure before Glue mutation leaves the old partition visible. S3 construction
and Glue publication remain separately observable, and an operator must reconcile the
rare ambiguous concurrency case rather than accepting last-writer-wins behavior.

The deterministic `run_id` owns both construction and publication claims. Retries of the
same run therefore converge even when invocation/request metadata changes; claim owner
labels and processing timestamps remain receipt metadata and do not alter the claim or
run identity. A different run cannot replace the month claim without the explicit ETag
authorization above.
