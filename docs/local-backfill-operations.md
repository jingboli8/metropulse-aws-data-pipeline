# Local backfill operations

Phase 2 is an offline workflow. It reads an existing verified source, writes generated
files beneath an ignored output root, and makes no network or AWS calls. The future AWS
runtime in the architecture documents remains planned.

## Run contract

Install the project and development dependencies, then use a fixed processing timestamp
and explicit pipeline version:

```powershell
.\.venv\Scripts\python.exe -m pip install ".[dev]"
.\.venv\Scripts\python.exe -m metropulse.backfill `
  --archive data\raw\metropt-3-dataset.zip `
  --output-root data\local-lake `
  --processing-timestamp 2026-09-10T00:00:00 `
  --pipeline-version 2.0.0 `
  --evidence artifacts\phase2-run.json
```

`--archive` accepts the official UCI ZIP and enforces its recorded size, member size, and
SHA-256 before processing. `--csv` accepts an existing CSV; pair it with
`--expected-source-sha256` when a trusted CSV digest is available. Neither mode downloads
or extracts the source. Paths stored in manifests are relative to the output root.

The processing timestamp is operational lineage supplied by the caller. A value without
an offset, such as the example above, is interpreted as UTC by this run contract and
stored with `Z`. This does not change the dataset timestamps: `event_timestamp_local`
remains timezone-naive and has timezone status `unknown`.

## Local lake layout

```text
data/local-lake/
  raw/source=metropt3/source_date=YYYY-MM-DD/data.csv
  staging/source=metropt3/year=YYYY/month=MM/day=DD/data.parquet
  quarantine/source=metropt3/source_date=YYYY-MM-DD/rejected.jsonl
  control/source=metropt3/source_date=YYYY-MM-DD/manifest.json
```

This local layout uses the concise `source=metropt3` value specified for Phase 2. The
planned AWS keys use `source=uci-metropt3` and add an immutable input identity where
concurrent event delivery requires it. Locally, the source checksum and pipeline version
are recorded inside every completion manifest, and the deterministic daily path has one
writer.

Daily raw CSV preserves the exact source header, row order, and field values. Staging is
one explicit-schema Snappy Parquet file per day and is never appended in place.
Quarantine is stable-key UTF-8 JSON Lines. When a day has zero rejected rows, its
quarantine file is omitted and the manifest records `absent_zero_rows`, a count of zero,
and no quarantine path or checksum. The manifest is written last as the completion
marker.

A malformed-width record with a parseable timestamp is routed unchanged and the core
validator quarantines it under `ROW_COLUMN_COUNT`. A record without a parseable timestamp
cannot supply a trustworthy partition date, so the splitter stops with a nonzero status
instead of guessing a partition.

## Resume and recovery

Resume is the default. The CLI first recreates a candidate daily raw file from the source
stream, then skips the day only if its manifest identity and configuration match and the
raw, staging, and any quarantine checksums, sizes, row counts, Parquet schema, and codec
all verify. File existence alone never causes a skip.

A missing, corrupt, or stale manifest/output causes that day to be rebuilt. The previous
manifest is removed before reconstruction and a new manifest is published only after
validation and row reconciliation succeed. Every data and manifest write uses a sibling
temporary file and an atomic replacement; interrupted temporary files are not treated as
complete. Use `--force` to rebuild every selected day deterministically.

For a focused recovery, add inclusive bounds:

```powershell
--start-date 2020-03-01 --end-date 2020-03-31
```

The command returns a nonzero status for a routing/schema failure, unreconciled day, or
failed partition. It reports processed, skipped, rebuilt, and failed day counts.

## Independent acceptance check

Run a separate inventory and Parquet inspection after the backfill:

```powershell
.\.venv\Scripts\python.exe -m metropulse.acceptance `
  --output-root data\local-lake `
  --evidence artifacts\phase2-acceptance.json
```

The verifier independently counts raw CSV, Parquet, quarantine, and manifest rows;
recomputes referenced checksums; checks every Parquet schema and codec; reconciles the
aggregate counts; and scans generated content and metadata for absolute Windows paths.
Detailed evidence stays under ignored `artifacts/` and is not committed.
