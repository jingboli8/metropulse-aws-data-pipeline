# Phase -1 dataset feasibility report

**Overall verdict: PASS WITH CONDITIONS.** The official dataset is accessible,
publicly licensed, internally readable, small enough for the proposed daily processing
unit, and successfully converts to Snappy Parquet. Conditions arise from the blank index
header, a source spelling (`DV_eletric`), timezone-naive timestamps, highly irregular
gaps around an otherwise approximately 10-second cadence, very small daily Parquet
objects for Athena, and reliance on one upstream UCI download endpoint.

This phase made no AWS calls, accessed no AWS credentials, and created no AWS resources.

## Decision matrix

| Category | Verdict | Evidence and condition |
|---|---|---|
| Source accessibility | **PASS** | The script downloaded the official UCI dataset-ID 791 URL successfully on 2026-09-04. |
| Authentication requirements | **PASS** | Direct HTTPS download completed without an account, API key, or token. |
| License/public portfolio use | **PASS** | The official UCI page identifies CC BY 4.0, which permits sharing/adaptation with attribution. Dataset attribution must be retained; a future code license is separate. |
| Privacy/sensitive data | **PASS** | UCI explicitly reports no sensitive data. Inspection found equipment telemetry fields only and no person-related columns. |
| File integrity | **PASS** | Full ZIP CRC test passed. SHA-256 was stable across the streaming and on-disk checks and on an idempotent rerun: `aab991a970e58210de853bb8078ce0e63abb4d9412fdc5c79792dae3d8e1721a`. |
| Schema usability | **PASS WITH CONDITIONS** | All 1,516,948 rows parse with no nulls or analogue conversion failures. Rename the blank first header to `record_index`; preserve or explicitly map UCI's `DV_eletric` spelling; document the timezone assumption. |
| Data volume | **PASS** | The archive is 218,381,995 bytes and the CSV is 218,300,507 bytes. This is modest for offline ingestion and profiling. |
| Partitioning feasibility | **PASS** | Calendar-date partitioning yields 212 partitions with 210 / 7,435 / 8,717 minimum/median/maximum rows. |
| Lambda daily-batch feasibility | **PASS WITH CONDITIONS** | A typical day is estimated at 1,069,953 CSV bytes and the maximum at 1,254,443 bytes, with fewer than 9,000 rows. One pre-partitioned day is comfortably small for one invocation. Phase 0 therefore assigns the one-time daily split to the local backfill CLI; Lambda never rescans the 218 MB monolithic CSV. |
| Parquet conversion feasibility | **PASS** | Exactly three representative dates converted with Snappy. All Parquet metadata row counts equal their CSV partition counts. |
| Athena suitability | **PASS WITH CONDITIONS** | Types normalize cleanly and Parquet works. Daily test files are only 11-197 KB, so querying one object per day would create a small-file pattern. Phase 0 keeps daily staging files and compacts curated data into roughly seven monthly units partitioned by year/month. |
| Reproducibility | **PASS** | Download URL, checksum, ZIP validation, safe extraction, profiling SQL, representative-day selection, and tests are scripted. Rerunning the downloader verifies and reuses the existing archive. |
| Risk of future source unavailability | **PASS WITH CONDITIONS** | UCI and a DOI provide durable provenance, but the binary is served from one current UCI URL and UCI does not publish the observed SHA-256 on the page. Phase 0 preserves the URL, DOI, attribution, and checksum and assigns an immutable, license-attributed source copy to the future S3 landing zone. |

## Integrity, size, and schema

The ZIP contains exactly two regular members:

| Member | ZIP compressed bytes | Uncompressed bytes |
|---|---:|---:|
| `Data Description_Metro.pdf` | 81,208 | 81,208 |
| `MetroPT3(AirCompressor).csv` | 218,300,507 | 218,300,507 |

The whole ZIP is 218,381,995 bytes (208.265 MiB); the extracted CSV is 218,300,507
bytes (208.188 MiB). The member sizes show that the source ZIP stores both files without
compression.

The CSV has exactly **1,516,948 data rows and 17 columns**. Its exact raw headers are:

```text
"", timestamp, TP2, TP3, H1, DV_pressure, Reservoirs, Oil_temperature,
Motor_current, COMP, DV_eletric, Towers, MPG, LPS, Pressure_switch,
Oil_level, Caudal_impulses
```

The empty first header is the record index. The recommended normalized schema is:

| Source field | Inferred | Recommended |
|---|---|---|
| empty first header | BIGINT | BIGINT named `record_index` |
| `timestamp` | TIMESTAMP | TIMESTAMP named `event_timestamp_local`; timezone-naive with timezone status explicitly `unknown` |
| `TP2`, `TP3`, `H1`, `DV_pressure`, `Reservoirs`, `Oil_temperature`, `Motor_current` | DOUBLE | DOUBLE |
| `COMP`, `DV_eletric`, `Towers`, `MPG`, `LPS`, `Pressure_switch`, `Oil_level`, `Caudal_impulses` | DOUBLE | BOOLEAN after explicit 0/1 validation |

Every digital column contains only `0.0` and `1.0`. Phase 0 maps `DV_eletric` to
`dv_electric` in staging/curated data while retaining the exact source name in lineage.

## Completeness and uniqueness

- Null count and percentage: **0 (0.000%) for every one of the 17 columns**.
- Non-numeric values in each of the seven expected analogue columns: **0**.
- Timestamp parsing failures: **0**.
- Duplicate full rows: **0**.
- Duplicate `record_index` values: **0**.
- Duplicate timestamp values: **0**.
- Out-of-order timestamp transitions: **0**; timestamps are strictly increasing.

## Timestamp range, cadence, and gaps

The exact timestamp range is **2020-02-01 00:00:00 through
2020-09-01 03:59:50**. This is slightly more precise than UCI's prose description of
“February to August 2020,” because the file extends four hours into September 1.

UCI's page is internally inconsistent: Dataset Information says logging was at 1 Hz,
while Additional Variable Information says 0.1 Hz. The data show a nominal cadence near
**0.1 Hz (one row about every 10 seconds)**, with logging jitter and gaps:

| Delta | Count | Share of all 1,516,947 deltas |
|---:|---:|---:|
| 10 s | 1,337,521 | 88.1719% |
| 9 s | 128,277 | 8.4563% |
| 12 s | 38,321 | 2.5262% |
| 13 s | 7,988 | 0.5266% |
| 11 s | 4,471 | 0.2947% |

There are 337 distinct observed delta values. The machine-readable profile retains the
complete distribution. For this feasibility study, a **significant gap** is defined as a
positive delta greater than 60 seconds: six times the modal 10-second interval and also
greater than twice that mode. There are **331** such gaps; their minimum, median, and
maximum sizes are **104 seconds, 3,141 seconds, and 172,918 seconds**, respectively.
The largest gap ends at 2020-04-27 01:12:49 and is about 48.0 hours. Gap handling must be
an explicit quality rule in the later architecture; missing time must not be imputed or
silently fabricated.

## Column distributions

### Analogue signals

Percentiles are empirical, not validity limits.

| Column | Min | P01 | P05 | P25 | P50 | P75 | P95 | P99 | Max | Mean |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TP2 | -0.032 | -0.024 | -0.016 | -0.014 | -0.012 | -0.010 | 9.590 | 10.352 | 10.676 | 1.3678 |
| TP3 | 0.730 | 7.876 | 8.138 | 8.492 | 8.960 | 9.492 | 9.980 | 10.140 | 10.302 | 8.9846 |
| H1 | -0.036 | -0.024 | -0.014 | 8.254 | 8.784 | 9.374 | 9.916 | 10.114 | 10.288 | 7.5682 |
| DV_pressure | -0.032 | -0.026 | -0.024 | -0.022 | -0.020 | -0.018 | -0.008 | 2.112 | 9.844 | 0.0560 |
| Reservoirs | 0.712 | 7.876 | 8.140 | 8.494 | 8.960 | 9.492 | 9.978 | 10.138 | 10.300 | 8.9852 |
| Oil_temperature | 15.400 | 48.825 | 52.325 | 57.775 | 62.700 | 67.250 | 73.525 | 76.175 | 89.050 | 62.6442 |
| Motor_current | 0.020 | 0.035 | 0.038 | 0.040 | 0.045 | 3.808 | 5.987 | 6.188 | 9.295 | 2.0502 |

No analogue value is NaN or infinite. UCI provides operational descriptions (for
example, approximate motor-current states and pressure-switch thresholds), but no hard
physical validity bounds. Therefore, the observed negative counts—TP2 1,275,474; H1
237,996; DV_pressure 1,444,001—are documented as small-offset empirical observations,
not declared impossible. TP3, Reservoirs, Oil_temperature, and Motor_current have no
negative values. Domain-reviewed rules are still needed before any “impossible value”
rejection policy is implemented.

### Digital signals

| Column | 0 count | 1 count |
|---|---:|---:|
| COMP | 247,328 | 1,269,620 |
| DV_eletric | 1,273,310 | 243,638 |
| Towers | 121,586 | 1,395,362 |
| MPG | 253,840 | 1,263,108 |
| LPS | 1,511,760 | 5,188 |
| Pressure_switch | 12,990 | 1,503,958 |
| Oil_level | 145,391 | 1,371,557 |
| Caudal_impulses | 95,406 | 1,421,542 |

No column is constant. At a documented 99% dominant-value threshold, `LPS` (99.658%
zero) and `Pressure_switch` (99.144% one) are near-constant. They remain meaningful rare
event signals and should not be dropped automatically.

## Daily partitions and Parquet experiment

The data produce **212 calendar-date partitions**. Daily row-count statistics are:

| Minimum | Median | Maximum | Largest date |
|---:|---:|---:|---|
| 210 | 7,435 | 8,717 | 2020-07-02 |

Allocating the actual CSV payload bytes in proportion to row counts estimates a typical
day at **1,069,953 bytes (1.020 MiB)** and the maximum day at **1,254,443 bytes
(1.196 MiB)**. This is an estimate because row text lengths vary.

Exactly three distinct representative days were converted to temporary Snappy Parquet:

| Selection | Date | CSV rows | Parquet rows | Estimated CSV bytes | Parquet bytes | Estimated CSV:Parquet ratio |
|---|---:|---:|---:|---:|---:|---:|
| Smallest | 2020-05-24 | 210 | 210 | 30,221 | 11,158 | 2.708x |
| Median-sized | 2020-02-05 | 7,437 | 7,437 | 1,070,241 | 165,331 | 6.473x |
| Largest | 2020-07-02 | 8,717 | 8,717 | 1,254,443 | 196,800 | 6.374x |

All three row-count checks passed. The files remain only under ignored
`data/staged/feasibility/`; the full dataset was not converted.

## Phase 0 resolutions and remaining constraints

1. Phase 0 preserves the timezone-naive value as `event_timestamp_local`, records timezone
   status as `unknown`, and derives `source_date` without UTC or daylight-saving inference.
2. Daily validation Parquet remains in staging; curated data is compacted into monthly
   Parquet partitioned by year/month for Athena.
3. Gaps greater than 60 seconds are batch warnings/metrics. Rows around gaps remain valid,
   and missing observations are not imputed.
4. Domain owners should approve physical validity bounds. Empirical offsets must not be
   mislabeled as impossible measurements.
5. The future private S3 landing zone holds the license-attributed immutable source copy;
   raw and derived dataset files remain outside Git.
6. Treat UCI's inconsistent maintenance note for the second failure window as unresolved
   source metadata, not a fact to silently correct.
