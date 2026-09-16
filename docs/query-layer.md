# Glue and Athena query-layer contract

Phase 5 defines metadata and query controls only. It creates no curated Parquet, Glue
partitions, or AWS resources during repository validation. The table is intentionally
empty until Phase 6 publishes a reconciled monthly run and explicitly registers its
approved `year` and `month` partition.

## Data boundary

Athena queries only the curated table `metropulse_<environment>.metropt3_curated` rooted
at:

```text
s3://<data-lake-bucket>/curated/metropt3/
```

Daily staging is not an analytics source. Lambda staging can retain several immutable
`input_id` attempts for one day, is governed by temporary retention, and requires
completion-marker selection. Raw, quarantine, validation-control, and audit objects are
also outside the query table.

The external Parquet table has the 19 normalized payload columns from the data contract
and partition columns `year string`, then `month string`. It uses Snappy, the Parquet
Hive SerDe, and explicit Parquet input/output formats. Glue table metadata cannot enforce
the Parquet fields' non-null contract; required-field validation remains upstream in the
pure transformation and monthly compaction contracts.

`event_timestamp_local` is an Athena `timestamp` that preserves a source wall-clock
value whose timezone is unknown. `event_timestamp_timezone_status` remains `unknown`.
Queries must not attach an offset or reinterpret the value as a universal timestamp.

## Partition publication

Partition projection is disabled. `run_id` is an immutable physical publication
identity, not a query partition. One explicitly registered `year`/`month` Glue partition
will point to:

```text
s3://<data-lake-bucket>/curated/metropt3/year=YYYY/month=MM/run_id=<approved-run-id>/
```

Phase 5 creates no `aws_glue_partition` resource. Phase 6 will register or replace a
partition only after its new run reconciles and its completion manifest is published.
`MSCK REPAIR TABLE` must not be used because automatic discovery could expose multiple
immutable runs for one month.

## Workgroup controls

The environment-scoped Athena workgroup enforces its own configuration:

- Athena engine version 3
- query metrics enabled
- Requester Pays disabled
- SSE-S3 query-result encryption
- expected results-bucket owner
- result prefix `s3://<athena-results-bucket>/workgroups/<workgroup-name>/`
- 268,435,456-byte (256 MiB) default per-query scan cutoff

The platform's Athena-results bucket already has Block Public Access, bucket-owner
enforced ownership, default SSE-S3, TLS-only access, and a 30-day result lifecycle.
Routine SQL filters both `year` and `month` so Athena can prune partitions and should
select only needed columns. The complete-history sampling-gap query is the deliberate
exception because removing month boundaries would hide continuity gaps; the workgroup
cutoff still bounds it.

## Terraform boundary and input order

`infra/query` is independent local Terraform state. It does not read remote Terraform
state and does not create or modify either bucket. Future authorized deployment order is:

1. Apply `infra/bootstrap` and publish a digest-pinned image.
2. Apply `infra/platform` to create the buckets and validation runtime.
3. Pass the platform output bucket names explicitly to `infra/query`.
4. Supply the expected 12-digit owner of the results bucket.
5. Apply `infra/query` only after reviewing the Glue schema and workgroup settings.

Required query-root inputs are AWS Region, environment, data-lake bucket name,
Athena-results bucket name, expected results-bucket owner, and reviewed tag values. The
scan cutoff defaults to 256 MiB and may be raised only through a reviewed variable.

## SQL and evidence boundary

Version-controlled queries under `sql/` are the source of truth; Phase 5 creates no
Athena named-query resources. Curated rows support sensor values, daily counts, analogue
aggregates, Boolean state rates, and observed timestamp intervals. They do not expose
input identity, object versions, checksums, valid/quarantine reconciliation, publication
identity, missing-date audit findings, overlaps, reversed boundaries, or scheduled-run
status. Query-derived counts cannot replace control-plane reconciliation.

Terraform format, validation, mock tests, and SQL contract tests are offline. Actual Glue
and Athena creation, partition visibility, result encryption, bytes scanned, cutoff
behavior, and query results require a later explicitly authorized AWS deployment. No
real Athena query evidence exists in Phase 5.
