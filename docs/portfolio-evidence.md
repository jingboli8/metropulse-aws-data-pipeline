# Portfolio evidence

This repository keeps a small, sanitized evidence summary in
[`docs/evidence/local-acceptance-summary.json`](evidence/local-acceptance-summary.json).
The source data, generated Parquet, full manifests, and machine-generated acceptance
artifacts remain ignored.

## What the evidence proves

- The verified local source contains 1,516,948 observations in 212 daily inputs.
- Daily validation produced zero quarantined rows, based on actual reconciliation rather
  than a configured expectation.
- Monthly compaction produced eight immutable local Parquet outputs for 2020-02 through
  2020-09. Their rows reconcile exactly to the source.
- Every curated column matched the 19-column contract, timestamps remained
  `timestamp[ms]` without a timezone, and every column chunk used Snappy.
- The chronological audit found 327 within-month gaps and four cross-month gaps over 60
  seconds, for 331 total. Gaps are quality observations and do not reject rows.
- An identical rerun preserved run identities, logical manifests, and Parquet hashes in
  the pinned local runtime.
- A user-run Linux amd64 container verification imported PyArrow 21.0.0 and all three
  handlers, passed deterministic synthetic smoke tests and image-hygiene checks, and
  reported an image size of 233,386,762 bytes (about 222.6 MiB).

The detailed per-month sizes and hashes are in
[`monthly-compaction.md`](monthly-compaction.md). Local backfill and recovery commands
are in [`local-backfill-operations.md`](local-backfill-operations.md).

## Evidence boundary

Infrastructure is represented by Terraform and validated with mocked providers. No AWS
resource has been deployed, and no S3, Lambda, Glue, Athena, EventBridge Scheduler,
CloudWatch, IAM, or ECR API has been verified against a real account. The SQL examples
are derived locally from Parquet and are not Athena execution results. A real deployment
is an optional, separately authorized Phase 9.

## Licensing boundary

Repository code is licensed under the [MIT License](../LICENSE). The MetroPT-3 dataset
is a separate work distributed by UCI under CC BY 4.0; the dataset is not included in
this repository. See [`data-source.md`](data-source.md) for attribution and retrieval
details.
