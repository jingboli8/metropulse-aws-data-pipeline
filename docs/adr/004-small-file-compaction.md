# ADR 004: Keep daily validation files and compact curated data monthly

- **Status:** Accepted
- **Date:** 2026-09-10

## Context

The verified dataset produces 212 daily partitions. Representative daily Snappy Parquet
files are about 11-197 KB, far below generic file-size guidance often used for large
Athena datasets. Querying hundreds of tiny daily objects adds metadata and request overhead,
but adding distributed compute for this small, fixed dataset would be disproportionate.

## Decision

Preserve daily raw CSV as replay inputs and produce one daily staging Parquet object per
validation boundary. An on-demand monthly process reads an explicitly approved set of
completed daily manifests and writes one deterministic Snappy Parquet object for each
observed month. Curated storage
is partitioned by `year` and `month`, never day. It retains `source_date` as a column.

The compactor does not append in place. It writes a complete deterministic run, validates
Parquet metadata and checksums, and publishes a completion manifest/catalog partition
only after:

```text
curated rows = sum(selected daily valid rows)
source rows = curated rows + associated quarantine rows
```

## Consequences

- Exactly 212 daily units become 8 monthly query units for 2020-02 through 2020-09,
  materially reducing object count while keeping validation/replay granular. September
  is a valid partial terminal month containing only 2020-09-01.
- Some monthly files may still be below generic 128 MB guidance. That is acceptable and
  proportionate for this dataset; chasing a target file size would add unnecessary
  infrastructure and reduce clarity.
- Reprocessing a daily input changes the manifest set and monthly run ID. Publication
  selects the approved complete run rather than modifying an existing Parquet object.
