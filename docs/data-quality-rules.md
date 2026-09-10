# Data-quality rules

## Rule classes

MetroPulse uses four distinct classes:

- **Row error:** reject only the affected row into quarantine. Other valid rows continue.
- **Batch error:** fail the input or monthly publication; no completion marker is written.
- **Warning:** preserve valid rows and complete processing, but log and emit a metric for
  investigation.
- **Metric:** measure behavior without deciding validity. An alarm may evaluate it.
- **Audit reconciliation:** compare independent counts or expected outputs. A mismatch is
  a batch error and an operational alarm, not an extra row rejection.

Failure and maintenance reference windows never participate in these rules. A valid
telemetry row inside a reported failure window remains valid telemetry.

## Row-level quarantine rules

All required fields are the 17 source fields documented in the data contract. Where a
structural error prevents field access, only applicable checks run. A row can carry more
than one rule ID. Duplicate rules reject every member of the duplicate group so the
result does not depend on row-processing order.

| Rule ID | Class | Condition | Result |
|---|---|---|---|
| `ROW_COLUMN_COUNT` | Row error | Parsed record does not contain exactly 17 fields. | Preserve original record and reject it. |
| `ROW_TIMESTAMP_PARSE` | Row error | `timestamp` cannot be parsed exactly as `YYYY-MM-DD HH:MM:SS`. | Reject row; do not invent a time. |
| `ROW_NUMERIC_PARSE` | Row error | `record_index` cannot parse as integer or any of the 15 signal values cannot parse as a numeric value. Parseable NaN/infinity proceeds to the dedicated analogue-domain rule. | Reject row with affected field names. |
| `ROW_REQUIRED_MISSING` | Row error | Any required source value is empty or null. | Reject row with missing field names. |
| `ROW_ANALOG_NONFINITE` | Row error | Any analogue field is NaN, positive infinity, or negative infinity. | Reject row with affected field names. |
| `ROW_DIGITAL_DOMAIN` | Row error | Any digital field is not exactly numeric `0` or `1`. | Reject row; only valid values convert to Boolean. |
| `ROW_SOURCE_DATE_MISMATCH` | Row error | Calendar date derived from the parsed source timestamp differs from `source_date` in the raw key. | Reject row and record both dates. |
| `ROW_DUPLICATE_RECORD_INDEX` | Row error | `record_index` occurs more than once within this input object. | Reject every row in that duplicate group. |
| `ROW_DUPLICATE_TIMESTAMP` | Row error | Parsed timestamp occurs more than once within this input object. | Reject every row in that duplicate group. |

Rules do not apply unverified physical minima or maxima. Negative pressure-like readings
observed in the verified source are not automatically impossible. Such bounds require an
authoritative domain source and a versioned contract change.

## Batch warnings and metrics

| Rule ID | Class | Initial definition | Processing effect |
|---|---|---|---|
| `BATCH_TIMESTAMP_ORDER` | Warning + metric | Count adjacent parsed timestamps that decrease in source order. | Complete valid output; report transition count. |
| `BATCH_SAMPLING_INTERVAL` | Warning + metric | Count positive adjacent intervals outside 9-13 seconds; publish the interval distribution summary. | Complete valid output; do not reject either row. |
| `BATCH_SIGNIFICANT_GAP` | Warning + metric | Count adjacent positive intervals greater than 60 seconds, the Phase -1 documented threshold. | Complete valid output; do not impute and do not reject rows around a gap. |
| `BATCH_LOW_ROW_COUNT` | Warning + metric | Daily input has fewer than 3,718 rows, approximately half the observed median of 7,435. | Complete if otherwise valid; report the sparse date. Threshold is configuration, not a row rule. |
| `BATCH_QUARANTINE_RATE` | Metric | Rejected rows divided by input rows, plus counts by rule ID. | Complete after reconciliation; CloudWatch alarm evaluates spikes. |
| `BATCH_DURATION` | Metric | End-to-end validation or compaction duration. | No validity effect. |

The cadence bands and low-row threshold are operational baselines for this fixed dataset.
They can be tuned through versioned configuration after evidence review; tuning never
rewrites source timestamps or missing periods.

## Batch errors

| Rule ID | Class | Condition | Result |
|---|---|---|---|
| `BATCH_SCHEMA_DRIFT` | Batch error | Header count, order, or exact source names differ from the contracted 17-column schema. | Stop before row publication; retain diagnostics and retry only after review/versioning. |
| `BATCH_EMPTY_INPUT` | Batch error + metric | Object has a header but no data records, or is zero bytes/unreadable as CSV. | Produce no staging output; record failure. |
| `BATCH_CHECKSUM_MISMATCH` | Batch error | Computed checksum differs from an expected manifest/S3 checksum when one exists. | Stop; do not publish outputs or completion. |
| `BATCH_INVALID_PARTITION_KEY` | Batch error | Raw key does not contain exactly one parseable `source_date=YYYY-MM-DD` segment. | Stop because row partition checks cannot be trusted. |
| `BATCH_OUTPUT_WRITE` | Batch error | Any deterministic staging, quarantine, or manifest write fails. | Leave no completion marker; retry reconstructs deterministic outputs. |

An empty input and schema drift are batch conditions because there is no trustworthy row
set to partially publish. The audit still records their metrics and failure status.

## Audit and reconciliation rules

| Rule ID | Class | Required assertion | Failure behavior |
|---|---|---|---|
| `AUDIT_DAILY_ROWS` | Audit reconciliation | `input_rows = staging_rows + quarantine_rows`. | No daily completion marker; alarm. |
| `AUDIT_DAILY_PARQUET` | Audit reconciliation | Staging Parquet metadata row count equals manifest valid-row count. | No daily completion marker; alarm. |
| `AUDIT_MONTHLY_ROWS` | Audit reconciliation | Sum of selected daily input rows equals curated rows plus associated quarantine rows. | Do not publish the monthly completion manifest/catalog partition; alarm. |
| `AUDIT_MONTHLY_VALID_ROWS` | Audit reconciliation | Curated rows equal the sum of valid rows from the exact selected daily manifests. | Do not publish; alarm. |
| `AUDIT_EXPECTED_DAYS` | Audit reconciliation + warning | The month's selected source dates and daily manifests agree with the raw inventory. | Mark month incomplete; do not silently omit raw inputs. |
| `AUDIT_SCHEDULED_OUTPUT` | Audit reconciliation | Expected monthly completion marker exists after the configured schedule grace period. | Missing-output alarm. |
| `AUDIT_OUTPUT_CHECKSUM` | Audit reconciliation | Stored output checksums match recomputed checksums during an audit when enabled. | Mark audit failed; alarm and investigate before reprocessing. |

Monthly compaction may legitimately have no quarantine rows. “As appropriate” means the
monthly audit selects quarantine counts from the same daily input identities as staging;
it never compares unrelated attempts or pipeline versions.

## Evaluation order and reporting

Validation first verifies object identity, checksum, key shape, and header. It then parses
records, applies required/type/domain rules, evaluates within-object duplicates, writes
deterministic staging and quarantine outputs, and performs reconciliation before marking
completion. Batch warnings are calculated over parseable timestamps and do not convert
otherwise valid rows into rejects.

Structured logs contain job, environment, pipeline version, input identity, rule ID,
counts, duration, and status. Raw records and high-cardinality object identifiers remain
out of metric dimensions. CloudWatch receives row totals, quarantine rate and rule counts,
gap/order/interval/low-volume metrics, reconciliation status, duration, and failure
counts. Terminal row totals are emitted only after the invocation wins conditional
completion, which limits double-counting on duplicate S3 delivery; attempt and error
metrics intentionally describe every invocation.
