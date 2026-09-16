# Implementation plan

Phase 0 ends with the architecture, schema, quality, idempotency, compaction, security,
operations, and cost contracts in this repository. No AWS resource is created in Phase 0.
Each later phase must stop at its stated boundary so evidence can be reviewed before the
next layer adds complexity.

## Phase 1: Core schema, validation, and transformation package

**Deliverables**

- Versioned Arrow schema, source-to-normalized mapping, typed domain models, and stable
  rule IDs matching the contracts.
- Pure functions for CSV record parsing, row validation, duplicate detection, source-date
  checks, Boolean conversion, batch summaries, and count reconciliation.
- Parquet writer configuration for one deterministic Snappy output, with no `boto3` or
  Lambda event dependency in core modules.

**Acceptance criteria**

- All 17 source fields map exactly as documented; `timestamp` remains timezone-naive,
  `DV_eletric` maps to `dv_electric`, and every valid row includes `source_date` and
  timezone status `unknown`.
- Every row and batch rule has a stable result type. Gaps and failure windows cannot
  quarantine valid rows. No physical min/max rule exists without a cited source.
- Input, valid, and quarantine counts reconcile in memory.

**Tests**

- Unit and property-focused tests for every parsing/domain rule, compound rejections,
  duplicate groups, timestamp semantics, source-date boundaries, non-finite values, and
  reconciliation failures.
- Golden Arrow-schema and Parquet metadata tests using small synthetic fixtures.

**AWS resources affected:** none.

**Stop point:** core tests, Ruff, type checks if adopted, and contract review pass; no CLI,
S3, Lambda, or Terraform work begins.

## Phase 2: Local daily backfill and Parquet generation

**Deliverables**

- Thin local CLI that verifies/reuses the official ZIP, safely reads the monolithic CSV,
  and writes 212 deterministic daily raw CSV objects locally before any upload.
- Streaming split manifests containing checksums, row counts, timestamps, source dates,
  splitter version, and original ZIP checksum.
- Offline execution of the Phase 1 validator and daily Snappy Parquet writer for local
  evidence. Production ingestion still uploads daily CSV to raw and lets S3 invoke the
  validator; local Parquet is an ignored test artifact.
- Independent local acceptance verification for row counts, checksums, Parquet schema
  and codec, portable paths, and aggregate reconciliation.

**Acceptance criteria**

- Exactly 1,516,948 source rows reconcile across 212 daily files without changing source
  values, inventing timestamps, or downloading the dataset again when the verified ZIP
  exists.
- Every record's derived date matches its daily path. Rerunning produces the same key
  plan and checksums or reports a deterministic reason for any metadata difference.
- Raw CSV, local Parquet, manifests under generated paths, and caches remain ignored.

**Tests**

- Unit tests for date routing, CSV quoting/newlines, deterministic naming, checksum reuse,
  and dry-run output; integration test against a small ZIP/CSV fixture.
- One full local reconciliation run against the ignored verified dataset, recorded as
  summarized evidence rather than committed raw/generated files.

**AWS resources affected:** none. Upload and AWS adapter behavior begin only in later
reviewed phases.

**Stop point:** local split and Parquet evidence reconcile; do not upload or implement a
Lambda adapter until review.

## Phase 3: S3/Lambda event adapter and quarantine path — implemented locally

**Deliverables**

- Thin parser for S3 `ObjectCreated` event records and an S3 adapter for version-aware
  reads, conditional control writes, deterministic uploads, and checksums.
- Validation Lambda handler that calls the Phase 1 core and writes staging, quarantine,
  claims, and completion manifests.
- Prefix/suffix notification contract limited to `raw/` and `.csv`, bounded retries,
  structured logs, and Embedded Metric Format output. Alarm resources and packaging stay
  in Phase 4.

**Acceptance criteria**

- Duplicate delivery, concurrent delivery, partial output failure, stale claims, and
  versioned reprocessing behave as ADR 003 specifies.
- Valid rows reach one daily Snappy Parquet object; rejected rows satisfy the quarantine
  contract; completion occurs only after daily reconciliation.
- Core modules remain importable and testable without `boto3` or Lambda event shapes.

**Tests**

- Unit tests for event decoding and adapter errors; mocked/fake-S3 contract tests for
  conditional claims, retries, exact keys, and source identities.
- Local handler tests for mixed valid/invalid files, malformed events, duplicate events,
  and injected failures between each write.

**AWS resources affected:** design/code references one data bucket, validation Lambda,
its IAM role/log group, S3 notification, and CloudWatch failure alarm; none is provisioned
until Phase 4.

**Stop point:** adapter, handler, and real-day fake-S3 acceptance pass locally; no AWS
deployment or packaging begins.

## Phase 4: Container packaging and Terraform infrastructure

**Status:** Complete locally; the user-run Linux amd64 container verification passed.

**Deliverables**

- Separate Terraform roots for an immutable, encrypted, scan-on-push ECR repository and
  for private data/Athena-results buckets, versioning, encryption, Block Public Access,
  TLS-only policies, lifecycle rules, least-privilege IAM, finite log retention, the
  digest-pinned Lambda, bounded retries, and the raw notification.
- Official digest-pinned Lambda Python 3.12 Linux amd64 container with a hash-pinned
  PyArrow wheel, narrow Docker context, handler command, and a PowerShell verification
  script for imports, deterministic processing, content, history, and image size.
- Environment inputs with no schedules. Common tags include project, environment,
  owner, managed-by, and purpose.
- Local state decision, signed provider locks for Windows/Linux amd64, mock-provider
  tests, and documented bootstrap, release, recovery, and destroy commands. State,
  plans, variable files, and image exports remain ignored.

**Acceptance criteria**

- Static and mock-provider tests contain only the approved Phase 4 services and restrict
  notification to the raw prefix plus CSV suffix. IAM policy checks show prefix-level
  access by role. An AWS plan remains explicitly unexecuted.
- Buckets reject public access and non-TLS access, use encryption at rest, and isolate
  Athena results. Lifecycle policies cover staging, temporary multipart uploads, and
  query results without expiring retained raw/curated data prematurely.
- The local container verifier passes before commit. Terraform destroy documentation
  requires backup review and platform-first teardown; force deletion defaults to false.

**Tests**

- Terraform format/init/validate and native mock-provider tests plus repository security
  invariants. No test contacts AWS.
- User-run local Docker build/import/processor/content/history verification. Development
  account plan/deploy evidence is deferred until separately authorized.

**AWS resources affected:** ECR, S3, Lambda, IAM, CloudWatch Logs, and S3 notification
resources. Glue, Athena, compaction, audit, and schedules may be added in their owning
phases.

**Stop point:** local Terraform and externally executed container verification pass; do
not plan/apply, push, deploy, or start compaction.

## Phase 5: Monthly compaction and Glue/Athena

**Deliverables**

- Monthly compaction core and thin scheduled Lambda adapter using exact completed daily
  manifest sets and deterministic run IDs/keys.
- Curated Snappy Parquet partitioned by `year` and `month`, publication manifest, Glue
  database/table, partition registration, and Athena workgroup with enforced result path
  and scan limits.
- Representative Athena SQL demonstrating row counts, date filtering, and signal queries
  without treating every row as a failure.

**Acceptance criteria**

- About 212 daily staging units compact to roughly seven monthly units for this dataset;
  no append-in-place writes occur. Monthly row equations pass before catalog publication.
- Glue/Athena sees only curated schema and partitions. Timezone status is visible and no
  query or schema claims UTC.
- Reruns with the same manifest set are idempotent; a changed input or pipeline version
  produces a new run identity.

**Tests**

- Unit tests for month selection, stable manifest ordering/run IDs, boundary dates,
  reconciliation, deterministic ordering, and partial failures.
- Parquet schema/row-count integration tests and, when authorized, Athena smoke queries
  with recorded bytes scanned and query results.

**AWS resources affected:** compaction Lambda and IAM/log group, Glue database/table,
Athena workgroup and named query-result prefix; EventBridge schedule remains disabled.

**Stop point:** curated reconciliation and Athena smoke evidence pass; do not enable
schedules or alarms.

## Phase 6: CloudWatch metrics, alarms, and scheduled audit

**Deliverables**

- Structured JSON logging and low-cardinality custom metrics for validation, compaction,
  quarantine rules, sampling/gaps, duration, and reconciliation.
- Audit Lambda that inventories raw and immutable manifests, checks daily/monthly output,
  and records audit results under control.
- CloudWatch alarms for invocation failures, quarantine spikes, reconciliation failures,
  and missing scheduled monthly output.
- EventBridge Scheduler definitions for monthly compaction and post-grace-period audit,
  disabled by default in development with bounded retries and event age.

**Acceptance criteria**

- Duplicate events do not double-count terminal data metrics where conditional completion
  can prevent it. Attempt/failure metrics remain truthful about retries.
- Alarm tests reach and recover from alarm state using synthetic failures. Logs contain
  correlation IDs and counts but no raw records, credentials, or secrets.
- Enabling a schedule requires an explicit environment variable and reviewed plan.

**Tests**

- Unit tests for metric/log payloads, audit inventory, grace periods, and alarm thresholds.
- When authorized, synthetic CloudWatch and schedule smoke tests followed by disabling
  schedules again in development.

**AWS resources affected:** EventBridge Scheduler schedules/roles, audit Lambda, IAM,
CloudWatch custom metrics, alarms, dashboard, and log groups.

**Stop point:** observability evidence passes and development schedules are disabled.

## Phase 7: GitHub Actions, end-to-end evidence, README polish, and teardown

**Deliverables**

- GitHub Actions for Ruff, tests, packaging, Terraform formatting/validation/security
  checks, with no raw data or long-lived AWS keys.
- Bounded end-to-end run using synthetic data first and the verified dataset only under an
  explicitly approved AWS run; captured manifests, CloudWatch/Athena evidence, cost notes,
  and architecture screenshots that contain no credentials.
- Final README walkthrough, trade-offs, limitations, attribution, teardown procedure,
  and portfolio talking points.

**Acceptance criteria**

- CI passes from a clean clone without dataset access for unit tests. Repository checks
  prove raw CSV/ZIP, Parquet, artifacts, caches, `.venv`, and Terraform state are untracked.
- End-to-end counts reconcile and alarms/audit are demonstrated. Athena result isolation
  and scan limits are visible. The README accurately describes the metro-train APU data.
- Teardown inventories named resources, disables schedules, empties only project-owned
  disposable buckets after review, runs `terraform destroy`, and verifies no project
  resources remain. Retention exceptions are explicitly documented.

**Tests**

- Clean-clone CI; synthetic end-to-end test; approved real-data smoke/backfill; final
  security, cost, Git tracking, and teardown verification.

**AWS resources affected:** all project resources for final evidence and teardown; no
other account resources are in scope.

**Stop point:** evidence is saved safely, the repository is clean of generated data and
state, and the documented teardown is complete or an explicit retained-resource list is
approved.

## Cost controls across phases

No exact future price is assumed. Cost is bounded through a small fixed dataset, daily
event batches, monthly rather than daily Athena files, short Lambda timeouts,
log retention, S3 lifecycle policies for staging and query results, Athena scan limits,
disabled development schedules, and project/environment tags for cost attribution.
Terraform plans and AWS billing tools should be reviewed before and after any authorized
deployment.
