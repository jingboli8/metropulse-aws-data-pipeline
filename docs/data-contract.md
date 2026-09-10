# Data contract

## Dataset identity and grain

The source is the [UCI MetroPT-3 dataset](https://archive.ics.uci.edu/dataset/791/metropt%2B3%2Bdataset)
(dataset ID 791), licensed CC BY 4.0. UCI says it contains no sensitive data. The verified
source has 1,516,948 rows, 15 sensor signals, and 212 source-date partitions. It describes
a compressor Air Production Unit on a metro train, not a manufacturing production line.

The grain is: **one row represents one timestamped observation of all 15 signals from
one Air Production Unit**. A row is telemetry, not a confirmed failure or defect event.
Published failure and maintenance windows may be modeled later as separate analytical
reference intervals; they are not labels in this schema and are not validation rules.

## Raw object contract

Each raw object is UTF-8 CSV with the original 17-column header and data rows from
exactly one derived `source_date`. The object key is:

```text
raw/source=uci-metropt3/source_date=YYYY-MM-DD/metropt3-YYYY-MM-DD.csv
```

The local splitter preserves source values and source names, including the blank first
header and `DV_eletric`. The immutable ZIP in landing remains the byte-level source of
truth. A raw-object manifest records source ZIP SHA-256, raw-object checksum, row count,
minimum and maximum timestamp, source date, and splitter version.

Raw objects are ordered by their source order. Order is checked but is not relied upon
for identity. Raw data is never committed to Git.

## Source-to-curated schema

Every source value is required. Arrow timestamps remain timezone-naive. Parquet uses a
timestamp logical type with `isAdjustedToUTC=false`; Athena exposes it as `timestamp`
without asserting a timezone. Analogue values use 64-bit floating point to avoid adding
unverified precision constraints. Digital source values must be exactly numeric `0` or
`1` before conversion to Boolean.

### Identity and time fields

| Source name | Normalized name | Arrow type | Parquet type | Athena type | Nullable | Description |
|---|---|---|---|---|---|---|
| blank first header | `record_index` | `int64` | `INT64` | `bigint` | No | Source row identifier. The blank header is explicitly mapped and retained in lineage. |
| `timestamp` | `event_timestamp_local` | `timestamp[ms]`, no timezone | `INT64` with timestamp logical type, `isAdjustedToUTC=false` | `timestamp` | No | Parsed source wall-clock value. The source provides no timezone and MetroPulse does not claim UTC. |

### Analogue sensor fields

| Source name | Normalized name | Arrow type | Parquet type | Athena type | Nullable | Description |
|---|---|---|---|---|---|---|
| `TP2` | `tp2` | `float64` | `DOUBLE` | `double` | No | Compressor pressure measurement, reported by UCI in bar. |
| `TP3` | `tp3` | `float64` | `DOUBLE` | `double` | No | Pressure at the pneumatic panel, reported by UCI in bar. |
| `H1` | `h1` | `float64` | `DOUBLE` | `double` | No | Pressure associated with the drop when the cyclonic-separator filter discharges, reported by UCI in bar. |
| `DV_pressure` | `dv_pressure` | `float64` | `DOUBLE` | `double` | No | Pressure drop when the air-dryer towers discharge, reported by UCI in bar. |
| `Reservoirs` | `reservoirs` | `float64` | `DOUBLE` | `double` | No | Downstream reservoir pressure, reported by UCI in bar. |
| `Oil_temperature` | `oil_temperature` | `float64` | `DOUBLE` | `double` | No | Compressor oil temperature, reported by UCI in degrees Celsius. |
| `Motor_current` | `motor_current` | `float64` | `DOUBLE` | `double` | No | Current of one phase of the three-phase compressor motor, reported by UCI in amperes. |

These descriptions capture source meaning, not physical validity bounds. The source does
not provide authoritative hard minima or maxima, so observed ranges and percentiles are
profiling metrics only.

### Digital/binary state fields

| Source name | Normalized name | Arrow type | Parquet type | Athena type | Nullable | Description |
|---|---|---|---|---|---|---|
| `COMP` | `comp` | `bool` | `BOOLEAN` | `boolean` | No | Electrical state of the compressor air-intake valve; UCI says active indicates no air intake. |
| `DV_eletric` | `dv_electric` | `bool` | `BOOLEAN` | `boolean` | No | Electrical control state of the compressor outlet valve. The downstream spelling is corrected while the source spelling remains in lineage. |
| `Towers` | `towers` | `bool` | `BOOLEAN` | `boolean` | No | State selecting which tower dries air and which drains removed humidity. |
| `MPG` | `mpg` | `bool` | `BOOLEAN` | `boolean` | No | Electrical state that starts the compressor under load through the intake valve. |
| `LPS` | `lps` | `bool` | `BOOLEAN` | `boolean` | No | Electrical low-pressure detection state. |
| `Pressure_switch` | `pressure_switch` | `bool` | `BOOLEAN` | `boolean` | No | Electrical state detecting discharge in the air-drying towers. |
| `Oil_level` | `oil_level` | `bool` | `BOOLEAN` | `boolean` | No | Compressor oil-level detection state; UCI says active indicates oil below the expected level. |
| `Caudal_impulses` | `caudal_impulses` | `bool` | `BOOLEAN` | `boolean` | No | Pulse-output state from the meter measuring air flow from the APU to the reservoirs. The source name is retained because no authoritative replacement name is supplied. |

### Derived query fields

| Source name | Normalized name | Arrow type | Parquet type | Athena type | Nullable | Description |
|---|---|---|---|---|---|---|
| Derived from `event_timestamp_local` | `source_date` | `date32` | `INT32` with date logical type | `date` | No | Calendar date of the timezone-naive source value. Used only for partitioning and reconciliation. |
| Contract constant | `event_timestamp_timezone_status` | `string` | `BYTE_ARRAY` with UTF-8 logical type | `string` | No | Always `unknown` until authoritative source timezone metadata exists. |

Curated S3 partition columns `year` and `month` are derived from `source_date` and
registered as Glue/Athena `string` partition columns in `YYYY` and `MM` form. They do not
change timestamp semantics. The Parquet payload retains `source_date` for filtering and
reconciliation.

## Timestamp semantics

The source range is `2020-02-01 00:00:00` through `2020-09-01 03:59:50`. Values have no
offset or timezone. Parsing preserves the wall-clock value as `event_timestamp_local`;
no `Z`, UTC conversion, local timezone assumption, or daylight-saving adjustment is
added. File metadata, control manifests, the Glue column description, and the explicit
`event_timestamp_timezone_status` field record `unknown`.

The observed cadence is approximately 0.1 Hz and is mostly 10-second intervals, despite
contradictory upstream wording that also mentions 1 Hz. Collection gaps are real missing
coverage. MetroPulse records them as batch metrics and does not synthesize or impute rows.

## Staging and curated contracts

Staging contains one Snappy Parquet object per successfully processed daily raw input and
uses the full normalized schema above. Curated data is a lossless monthly compaction of
valid staging rows, ordered deterministically by `event_timestamp_local`, then
`record_index`. Curated objects use Snappy and are physically partitioned by `year` and
`month`, not day.

The schema has a version such as `1.0.0`. Renames, type changes, new required fields, or
semantic changes require a new major contract and pipeline version. A compatible new
nullable field requires a minor version. Source-header drift fails the fixed raw
contract and is not silently absorbed.

## Lineage and reconciliation

Each validation manifest records source bucket, key, version ID/ETag/checksum, raw
checksum, input identity, pipeline and schema versions, timestamps, input/valid/rejected
counts, output keys, and output checksums. Each compaction manifest lists the exact daily
manifests consumed and records monthly counts and checksums.

The mandatory equations are:

```text
daily input rows = daily staging rows + daily quarantine rows
monthly source rows = monthly curated rows + monthly quarantine rows
monthly curated rows = sum(valid rows in the selected daily manifests)
```

The second equation counts quarantine rows associated with the selected month's raw
inputs; quarantine data is not copied into curated Parquet. A failed equation prevents
publication and raises an audit failure.

## Quarantine record contract

Quarantine uses compressed UTF-8 JSON Lines so malformed CSV records and structured
rejection metadata can coexist. One JSON object represents one rejected source record.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `original_record` | string | No | Original CSV record text as read, without correction or coercion. |
| `parsed_fields` | object or null | Yes | Best-effort source-name/value mapping when the record can be structurally parsed. |
| `rule_ids` | array of string | No | Stable row-rule IDs; at least one value. |
| `rejection_reasons` | array of string | No | Human-readable reasons aligned with `rule_ids`. |
| `source_bucket` | string | No | Raw S3 bucket name. |
| `source_object_key` | string | No | Raw S3 object key. |
| `source_object_version_id` | string or null | Yes | S3 version ID when versioning supplies one. |
| `source_object_etag` | string or null | Yes | S3 ETag when available; it is not assumed to be a content hash. |
| `source_object_checksum` | string or null | Yes | S3 or manifest checksum and algorithm when available. |
| `source_row_number` | int64 | No | One-based physical data-record number after the header. |
| `source_date` | date string | Yes | Expected date from the key, or null if that key is malformed. |
| `processing_timestamp` | ISO 8601 string | No | UTC operational time at which rejection was produced. This does not alter source time. |
| `pipeline_version` | string | No | Immutable code/build version used for processing. |

At least one of version ID, checksum, or ETag must identify the source object. Quarantine
records may contain telemetry and lineage but never AWS credentials or secret values.
