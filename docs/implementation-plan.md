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

## Phase 5: Glue Data Catalog and Athena query contracts

**Status:** Implemented and locally validated as an offline query-definition phase.

**Deliverables**

- Separate Terraform root for an explicit Glue database and curated Parquet table plus an
  Athena workgroup with enforced encrypted results and a per-query scan cutoff.
- Curated-only schema with `year` and `month` partition columns, no crawler, no partition
  projection, and no partitions before curated output exists.
- Version-controlled Athena SQL for daily counts, analogue aggregates, digital-state
  rates, and observed timestamp gaps without treating telemetry rows as failures.
- Offline Terraform mock tests and repository contract tests. No AWS call or deployment
  evidence is part of this phase.

**Acceptance criteria**

- The Glue table exactly matches the normalized Parquet contract, catalogs only the
  curated root, and has no registered partitions.
- Athena workgroup settings override clients, use SSE-S3 in the isolated results bucket,
  enable metrics, and cap each query at 256 MiB by default.
- SQL preserves timezone-unknown semantics, uses explicit columns, and distinguishes
  row-derived observations from control-plane reconciliation evidence.

**Tests**

- Terraform format/init/validate and mocked-provider assertions for catalog, table,
  workgroup, result controls, tags, and excluded resources.
- Offline repository tests for exact schema/partition order and SQL safety contracts.

**AWS resources affected:** definitions for one Glue database, one Glue external table,
and one Athena workgroup. Nothing is created during this phase.

**Stop point:** offline query-layer contracts pass; do not compact data, register
partitions, deploy, or claim real Athena query evidence.

## Phase 6: Monthly compaction and curated partition publication

**Status:** Implemented and locally validated; no AWS deployment or partition exists.

**Deliverables**

- Monthly compaction core and thin adapter using exact completed daily manifest sets,
  deterministic run identities, and immutable curated keys.
- Snappy Parquet publication ordered by local event timestamp and record index, monthly
  completion manifests, checksum verification, and row reconciliation.
- Explicit Glue year/month partition creation or replacement pointing to one approved
  immutable `run_id` only after completion succeeds.

**Acceptance criteria**

- Exactly 212 daily validation units compact to 8 monthly units for 2020-02 through
  2020-09; September is a valid partial terminal month containing only 2020-09-01, and no
  append-in-place write occurs.
- Curated rows equal the valid rows from the exact selected daily completions, and source
  rows equal curated plus associated quarantine rows.
- Repeated identical inputs produce the same run identity. A changed input set publishes
  a new immutable run before changing the catalog partition location. S3 completion and
  Glue publication are ordered but cannot form a cross-service atomic transaction.

**Tests**

- Unit tests for selection, ordering, run identity, reconciliation, publication order,
  partial failure, and partition replacement.
- Full local Parquet schema, codec, row-count, and representative SQL-fixture checks.

**AWS resources affected:** future compaction Lambda/role/log group and Glue partition
updates. No EventBridge schedule is enabled.

**Stop point:** local compaction and partition-publication contracts pass; do not deploy,
schedule, or add alarms.

## Phase 7: On-demand compaction operations and scheduled integrity audit

**Status:** Complete locally; external Linux amd64 container verification passed and no
AWS resource was deployed.

**Deliverables**

- Exact immutable selection approval and audit-inventory publication CLIs with no bucket
  discovery.
- An on-demand compaction Lambda and a read-only full-checksum audit Lambda using the
  same digest-pinned image with different commands.
- Structured JSON logs and low-cardinality EMF metrics for validation, compaction, and
  audit outcomes.
- Separate least-privilege roles, finite log groups, failure/drift alarms, a compact
  dashboard, and a weekly audit schedule disabled by default.

**Acceptance criteria**

- Unique invocation owner tokens prevent two same-run invocations from owning one claim;
  verified completed work can return a no-op before reacquiring a claim.
- The scheduled audit reads one exact pinned inventory, reports known historical gaps as
  quality observations, and fails on broken evidence, reconciliation, or catalog drift.
- Enabling the schedule requires exact inventory inputs. No heartbeat alarm is defined:
  CloudWatch cannot express the required weekly cadence plus one-day grace within its
  seven-day maximum evaluation window.

**Tests**

- Unit tests for owner-token concurrency, selection/inventory identity, handlers,
  continuity, metric/log payloads, IAM, lifecycle, and schedule/heartbeat behavior.
- Terraform native tests with a mocked provider and an external offline container smoke
  before commit. Real service evidence remains a later authorized deployment concern.

**AWS resources affected:** definitions for compaction/audit Lambdas and roles,
EventBridge Scheduler and its invocation role, CloudWatch alarms/dashboard/log groups,
and the existing platform control-evidence lifecycle. Nothing is applied in Phase 7.

**Stop point:** offline evidence and external container verification passed; deployment
and schedule enablement remain separately authorized work.

## Phase 8: Account-free CI and portfolio readiness

**Status:** Implemented locally. The workflow cannot become GitHub evidence until the
repository is pushed and GitHub Actions completes successfully.

**Deliverables**

- GitHub Actions for Ruff, offline pytest, Markdown checks, Terraform
  formatting/validation/mock tests, and PowerShell parsing with no AWS identity.
- Strict `full_data` test separation so a clean clone needs no ignored dataset while the
  explicit local acceptance path remains available.
- Recruiter-oriented README, MIT code license, sanitized local evidence, and local
  Parquet-derived SQL examples with clear evidence boundaries.

**Acceptance criteria**

- Clean-clone commands exclude the marked full-data test, need no AWS account, and make
  no live AWS calls. Fresh runners may download pinned tools and dependencies from their
  official sources.
- Repository checks prove datasets, generated Parquet/evidence, caches, virtual
  environments, and Terraform state/plans remain untracked.
- Public documentation distinguishes local implementation, external container
  verification, mocked Terraform definitions, and the absence of real AWS evidence.

**Tests**

- Python 3.11 and 3.12 tests excluding `full_data`; Ruff and Markdown contracts;
  Terraform 1.16.2 validation and mocked tests across all four roots; PowerShell parsing;
  CI and repository-hygiene contract tests.

**AWS resources affected:** none.

**Stop point:** local CI contracts and portfolio evidence pass; do not create a remote,
push, deploy, or begin optional Phase 9 without separate authorization.

## Optional Phase 9: Real AWS deployment and teardown evidence

This phase is intentionally separate. If authorized, it would review a plan, deploy only
project-owned resources, push a digest-pinned image, publish exact selections and audit
inventory, run bounded service acceptance, capture sanitized Glue/Athena/Scheduler/
CloudWatch evidence, and execute the documented teardown. It would require real AWS
credentials, account APIs, cost review, and explicit approval. None of that evidence
exists today.

**AWS resources affected:** potentially all project resources, only after authorization.

**Stop point:** either verified teardown or an explicit retained-resource inventory.

## Cost controls across phases

No exact future price is assumed. Cost is bounded through a small fixed dataset, daily
event batches, monthly rather than daily Athena files, short Lambda timeouts,
log retention, S3 lifecycle policies for staging and query results, Athena scan limits,
disabled development schedules, and project/environment tags for cost attribution.
Terraform plans and AWS billing tools should be reviewed before and after any authorized
deployment.
