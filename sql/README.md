# Athena SQL contracts

These queries target Athena engine version 3 and use the development reference
`metropulse_dev.metropt3_curated`. For another environment, replace only the database
name with `metropulse_<environment>`; the table name remains `metropt3_curated`.

Phase 5 defines an empty table. Run these queries only after Phase 6 has published and
explicitly registered approved curated year/month partitions. Routine aggregate queries
filter both partition columns. `sampling_gaps.sql` intentionally scans complete curated
history so its `lag` window preserves cross-day and cross-month continuity. The 256 MiB
workgroup cutoff still bounds that scan.

`event_timestamp_local` is a timezone-unknown wall-clock value. Queries do not convert
it or attach a timezone, and they do not impute absent observations. Finite negative
analogue readings remain measurements rather than unsupported anomaly labels.

## Facts available from curated rows

- sensor values
- daily observation counts
- analogue aggregates
- Boolean state rates
- observed timestamp intervals derived from adjacent rows

## Control-plane facts unavailable from the curated table

- input identity and source object versions
- raw, staging, quarantine, or curated checksums
- valid/quarantine reconciliation
- approved publication identity
- missing-calendar-date audit findings
- overlapping or reversed partition-boundary findings
- scheduled-run status

Query-derived row counts and intervals do not prove checksum integrity, publication
approval, or reconciliation. Those facts remain in completion manifests and global audit
artifacts; the scheduled-audit contract checks them outside the Athena table.

## Representative local results

The examples below were computed with PyArrow 21.0.0 from the eight ignored curated
Parquet files. They show what the version-controlled SQL should return for the same
published rows, but they are not Athena results. No Athena execution ID, bytes-scanned
value, runtime, or cost exists.

For the first three dates selected by `row_counts_by_date.sql`:

| source_date | observation_count |
|---|---:|
| 2020-02-01 | 7144 |
| 2020-02-02 | 7031 |
| 2020-02-03 | 8716 |

For 2020-02-01, `analogue_sensor_aggregates.sql` produces these values when rounded to
six decimal places:

| avg_tp2 | min_tp2 | max_tp2 | avg_tp3 | avg_h1 | avg_dv_pressure | avg_reservoirs | avg_oil_temperature | avg_motor_current |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.532965 | -0.028000 | 10.532000 | 8.968314 | 8.404471 | -0.019066 | 8.970930 | 55.748117 | 1.292689 |

For the same date, representative rates from `digital_state_rates.sql` are:

| comp | dv_electric | towers | mpg | lps | pressure_switch | oil_level | caudal_impulses |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.939250 | 0.060750 | 0.969205 | 0.939250 | 0.000000 | 0.999580 | 1.000000 | 1.000000 |

The complete-history `sampling_gaps.sql` contract yields 331 intervals over 60 seconds
from these local rows. Its first result is the 307-second interval from
`2020-02-01 12:48:40` to `2020-02-01 12:53:47` at `record_index=46540`. This count
reconciles to 327 within-month and four cross-month intervals. It is a source-quality
observation, not a defect label or checksum/publication proof.
