# Scheduled operations contract

## Static-source boundary

MetroPT-3 ends on 2020-09-01. Compaction is an on-demand historical publication action;
only the integrity audit is scheduled. The `infra/operations` Terraform root defines
resources but has not been applied. The weekly schedule defaults to disabled.

## Selection authority and compaction invocation

The operator CLI accepts reviewed completion-marker references, fetches only those exact
objects, validates their immutable identities, builds canonical
`metropulse-monthly-selection-v1`, recomputes the run ID, and conditionally publishes:

```text
control/compaction/source=metropt3/year=YYYY/month=MM/run_id=<run-id>/selection.json
```

Identical content is an idempotent no-op; conflicting content fails. A version ID is the
preferred returned pin. Without versioning, SHA-256 pins the bytes. ETag remains opaque.
No approval path lists S3.

The on-demand event contract `metropulse-compaction-invocation-v1` contains the target
year/month, exact selection bucket/key/identity, optional expected current Glue
location, optional explicitly authorized prior publication-claim ETag, and an aware
processing timestamp. Inline or unpinned selections fail. Lambda derives a unique
`owner_token` as `compaction:<aws-request-id>`; this value can appear in claims, receipts,
and logs but never affects the run, selection, curated output, or completion identities.

A retry first checks for a publication receipt. It returns `verified_no_op` only after
the receipt, selection, curated output, completion bytes, and current Glue location all
match. Otherwise it conditionally acquires construction and publication claims. A stale
month-publication claim is never automatically taken over. Explicit recovery pins the
reviewed prior ETag. S3 and Glue are not atomic, and absolute fencing against a paused
former publisher is not claimed.

## Audit inventory authority

Contract `metropulse-scheduled-audit-inventory-v1` is compact canonical UTF-8 JSON with
sorted keys and chronological month entries. Its lowercase SHA-256 is `inventory_id` and
determines the immutable key:

```text
control/audit/source=metropt3/inventory_id=<inventory-id>/inventory.json
```

It records the known source range, ordered expected months, exact selected run per month,
pinned selection/completion/publication references, curated key/checksum/size, Glue
values/location, row counts, timestamp endpoints, within-month gap count, coverage,
known missing dates, and schema/manifest/inventory versions. Conditional publication is
idempotent only for identical bytes. The audit Lambda is read-only and cannot publish or
repair inventories.

## Full-checksum audit

The `metropulse-scheduled-audit-invocation-v1` event supplies the exact pinned inventory,
`full_checksum`, and either complete Scheduler context or a caller-supplied aware audit
time. The bounded audit fetches only exact references, recomputes object hashes, reopens
every curated Parquet object, verifies schema, timestamp semantics, Snappy, ordering,
row counts and boundaries, reads exact Glue partitions, reconciles rows, and evaluates
cross-month continuity using the same pure rules as the local audit.

Fatal conditions include invalid inventory/evidence, missing or corrupt objects,
unsupported contracts, and omitted/duplicate expected months. Row equation failures are
reported separately from Glue publication drift. Gaps, missing source dates, partial
terminal coverage, and represented overlap/reversal remain quality observations. For the
verified data, `327` within-month gaps plus `4` cross-month gaps equals `331`; the generic
core calculates the result and does not hard-code it.

## Scheduler, alarms, and IAM

EventBridge Scheduler targets only the audit Lambda at `cron(0 6 ? * MON *)` in UTC with
flexible window off, maximum event age 3,600 seconds, and two retries. Its fixed input
uses supported Scheduler context attributes. The schedule can be enabled only with a
canonical key and exact immutable identity.

Compaction runs at 2,048 MiB memory, 300 seconds, 1,024 MiB `/tmp`, and reserved
concurrency one. Audit uses 1,024 MiB, 300 seconds, 512 MiB `/tmp`, and reserved
concurrency one. Each has a separate finite-retention log group and role. Audit is
read-only; compaction cannot execute Athena or delete partitions; Scheduler can invoke
only audit. No role has `s3:ListBucket`, broad wildcard Allow, ECR runtime permissions,
or direct CloudWatch metric API permission.

Failure and drift alarms treat missing metrics as non-breaching. A heartbeat alarm is
omitted because CloudWatch allows at most a seven-day evaluation window for these
periods; that cannot provide the required grace beyond a weekly cadence. See the official
[missing-data behavior](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/alarms-and-missing-data.html),
[alarm evaluation limits](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Alarms.html),
and [Scheduler context attributes](https://docs.aws.amazon.com/scheduler/latest/UserGuide/managing-schedule-context-attributes.html).

## Evidence boundary

Unit tests, mocked-provider Terraform tests, and deterministic fake-S3/fake-Glue tests
make no AWS calls. The user completed the external Linux amd64 verification with:

```powershell
.\scripts\verify_lambda_container.ps1
```

The run confirmed PyArrow 21.0.0, imports and configured commands for the validation,
compaction, and audit handlers, the one-row validation smoke, and an operations smoke
that reconciled two rows and one timestamp gap. The task-root audit inspected 762
entries; the Lambda default-user configuration and Docker-history secret scan also
passed. The loaded image was 233,386,762 bytes (approximately 222.6 MiB). The operations
smoke reported deterministic synthetic fixture inventory and run identities; they are
test evidence, not production identities. The local image ID is likewise not a durable
deployment digest because BuildKit provenance or attestation can change it. Durable
build inputs are the pinned base-image digest and hash-pinned dependency; deployment
must use the eventual ECR image digest.

Real Lambda creation, Scheduler delivery, CloudWatch telemetry/alarm behavior, S3 reads,
Glue mutations, image pull, and schedule execution require a later authorized AWS
deployment. None is claimed by this repository state.
