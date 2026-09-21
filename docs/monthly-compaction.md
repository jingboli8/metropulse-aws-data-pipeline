# Monthly compaction operations

Phase 6 implements deterministic monthly compaction locally and defines thin S3/Glue
adapters. Nothing is deployed. Glue and Athena still have no real partitions or query
evidence.

## Selection and layout

The authoritative input is an explicit selection containing one pinned completion
marker and one staging checksum per source date. The AWS adapter reads exact keys and
never discovers approval by listing immutable attempts. The local adapter translates
the existing one-file-per-day manifests into the same model for acceptance testing.

| Object | Key |
|---|---|
| Curated data | `curated/metropt3/year=YYYY/month=MM/run_id=<run-id>/part-00000.snappy.parquet` |
| Selection | `control/compaction/source=metropt3/year=YYYY/month=MM/run_id=<run-id>/selection.json` |
| Construction claim | `control/compaction/source=metropt3/year=YYYY/month=MM/run_id=<run-id>/claim.json` |
| Completion | `control/compaction/source=metropt3/year=YYYY/month=MM/run_id=<run-id>/completed.json` |
| Publication receipt | `control/compaction/source=metropt3/year=YYYY/month=MM/run_id=<run-id>/published.json` |
| Month publication claim | `control/compaction/source=metropt3/year=YYYY/month=MM/publication/claim.json` |

Glue values are `["YYYY", "MM"]`. Its location is the exact run directory and ends in
`/`. A publication receipt is historical and may later be superseded by another approved
run.

## Verified source-month contract

| Month | Dates | Rows | Coverage |
|---|---:|---:|---|
| 2020-02 | 28 | 214850 | Missing 2020-02-29 |
| 2020-03 | 31 | 230448 | Complete |
| 2020-04 | 29 | 198734 | Missing 2020-04-26 |
| 2020-05 | 31 | 212800 | Complete |
| 2020-06 | 30 | 216514 | Complete |
| 2020-07 | 31 | 222638 | Complete |
| 2020-08 | 31 | 220434 | Complete |
| 2020-09 | 1 | 530 | Partial terminal month ending 2020-09-01 |

The aggregate is 212 selected daily inputs, 8 monthly files, and 1,516,948 rows. Missing
dates and the terminal partial month are coverage findings. They are not validation
failures and are never imputed.

## Local full-data evidence

The ignored full-data run used PyArrow 21.0.0 and the fixed writer contract. Every file
has the exact 19-column schema, `timestamp[ms]` without a timezone, Snappy on every
column chunk, and row groups bounded to 65,536 rows.

| Month | Bytes | SHA-256 |
|---|---:|---|
| 2020-02 | 5779520 | `c74eb6ba7e35e5d2399a52c32ccc5a0f98d9b38cae8773c90cd33330c634df95` |
| 2020-03 | 6438549 | `caad791207785074698b51d97bacd07286b08b8520a45eff98a36be312c406f1` |
| 2020-04 | 5278142 | `925eac0ad40ba51db62ced00e40a7ffbbfd17be4d792707da88cf8e89da7fdb1` |
| 2020-05 | 5696035 | `25ed29f281f628460bde9a5cad55d5209de90be11c11467c38c2b87525daa8b3` |
| 2020-06 | 5701843 | `c6404a32cd8bbfdf98ca937947c527435e9caf9e13b3db43bf3c091fff7e6d54` |
| 2020-07 | 6227201 | `0c7a5289a8c43a35b602d61ca8e27d5fab1baa232d8da82d5c77456d93a33ca1` |
| 2020-08 | 6195644 | `06cd3aea67030df631947aea77a2f5094d2cecaf072fbe813ea9f58611900311` |
| 2020-09 | 24160 | `76c96c04772c65132b56c55be211ebbb25529c8ae86ffe487e6aedc58dabcae0` |

The first construction run took 25.068 seconds. The final two current-code integrity
runs took 38.278 and 45.470 seconds and resumed all eight runs without rewriting them. Their
run IDs, sizes, and hashes were identical. The monthly outputs contain 327 gaps greater
than 60 seconds; 4 cross-month boundaries bring the full chronological total to 331.
There were no overlapping or reversed month boundaries. These durations are local
observations, not service-performance claims.

The largest verified month has 230,448 rows; selected daily compressed inputs total less
than 6.7 MB per month, and the largest curated file is less than 6.5 MB. The default
adapter bounds are 31 inputs, 128 MiB compressed input, 500,000 input rows, and 128 MiB
output. Compaction reads only explicitly selected keys and performs no bucket scan. The
current in-memory Arrow strategy is proportionate to this dataset; Phase 7 must confirm
deployed Lambda memory, `/tmp`, timeout, concurrency, logs, metrics, encryption/IAM, and
container rebuild settings before scheduling it.

## Local command

Run from the repository root with a fixed operational evidence timestamp:

```powershell
.\.venv\Scripts\python.exe -m metropulse.compaction_cli `
  --lake-root data/local-lake `
  --processing-timestamp 2026-09-16T00:00:00Z `
  --evidence artifacts/phase6-compaction-acceptance.json
```

The timestamp is excluded from the run identity and Parquet bytes. Identical selected
evidence and writer contracts produce the same run IDs. Existing completed local objects
must match byte-for-byte; conflicting content is not overwritten.

## Publication and recovery

The processor writes selection, curated Parquet, and completion in that order. It then
acquires the strict month-publication claim, verifies ownership, updates or creates the
Glue partition, reads the result back, rechecks ownership, and writes the historical
receipt. A failed replacement before Glue mutation leaves the old approved partition
visible. An ambiguous response is resolved only through read-back. A stale publication
claim requires operator reconciliation and is never taken over automatically.

S3 object creation is atomic, but S3 and Glue are not one transaction. Phase 7 supplies
an on-demand Lambda, unique invocation owner tokens, bounded concurrency, metrics,
alarms, and the scheduled cross-month audit. Static historical compaction is not
scheduled.
