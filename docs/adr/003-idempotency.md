# ADR 003: Use deterministic S3 identities and conditional control markers

- **Status:** Accepted
- **Date:** 2026-09-10

## Context

S3 event notifications and Lambda retries can deliver the same input more than once, and
two invocations can overlap. Processing may fail after one output is written. MetroPulse
needs safe retry and deliberate versioned reprocessing without adding DynamoDB or an
orchestration service.

## Decision

Build a canonical input identity from:

```text
source bucket + source key + object version ID/checksum/ETag + pipeline version
```

All available immutable identifiers are recorded. Identity preference is version ID,
then an object checksum, then ETag. ETag is treated as an opaque version token, not
assumed to be an MD5 checksum. `input_id` is the SHA-256 of the length-delimited canonical
identity. A monthly `run_id` hashes target year/month, the sorted completed daily input
IDs, schema version, and pipeline version.

Validation follows this protocol:

1. Read the exact source version and compute/verify available checksums. If a completed
   marker for `input_id` exists and matches the expected outputs, return success.
2. Create `control/validation/input_id=<id>/claim.json` with S3 conditional creation
   (`If-None-Match: *`). The claim records owner token, start/expiry time, and source
   identity.
3. If an active claim exists, the duplicate exits or is retried. A claim is not stale
   until longer than the configured maximum Lambda lifetime plus safety margin. A stale
   claim can be replaced only with an ETag precondition so one recovery owner wins.
4. Write staging and quarantine to deterministic `input_id` keys. Never append to or
   mutate a Parquet file in place. Reconcile counts and verify output metadata/checksums.
5. Conditionally create immutable `completed.json`. Only the owner that publishes this
   marker emits terminal input/valid/quarantine row metrics. Attempt and error metrics may
   still count every invocation. Mark the claim completed or let lifecycle policy expire it.

Phase 3 fixes claim lifetime at 16 minutes, one minute beyond Lambda's maximum execution
time. The deterministic data-processing timestamp is the source object's `LastModified`
value in UTC, with S3 event time as fallback; the operational current-time clock affects
only claim expiry. Completion verification recalculates referenced object SHA-256 values
and checks their sizes and checksum metadata. An active claim fails retryably, while a
stale claim is replaced only through `If-Match` on its current ETag.

The stale threshold prevents a previous live validation Lambda from writing after
takeover. A retry after partial validation failure rewrites only its deterministic
outputs and repeats verification; absence of `completed.json` means they are not
published.

Monthly compaction uses immutable run output plus construction and month-publication
claims with a unique invocation `owner_token`. A matching publication receipt is checked
before a retry attempts a new claim, so fully completed work can return a verified no-op.
Two live invocations for one run cannot both own a claim. An interrupted construction
claim and any stale month-publication claim require explicit reconciliation; the latter
can be replaced only with a reviewed ETag precondition. The deterministic `run_id` never
serves as claim ownership. Glue partition registration occurs only after monthly
completion.

Versioned reprocessing is explicit. A new S3 version/checksum/ETag or pipeline version
creates a different input ID and output path. The selected daily manifest set creates a
different monthly run ID. Audit/catalog logic chooses one approved completed version and
does not union versions accidentally.

## Consequences

- Duplicate delivery becomes a cheap no-op after completion, and concurrent work has one
  publisher.
- S3 holds both data and simple coordination state, keeping the service list focused.
- Claim expiry and conditional-write behavior require focused race and partial-failure
  tests. Operators need an audited procedure for approving a reprocessing version.
- Terminal business metrics avoid practical double-counting, while invocation metrics
  correctly show retries and duplicates.
