# Structured observability contract

## Logs

The adapter writes one JSON object per log line. Every operational event contains
`event_name`, `environment`, and `pipeline_version`; record-level events add AWS request
ID when supplied, source bucket/key, record index, processing identity when known,
outcome, counts, and duration when applicable.

| Event name | Meaning |
|---|---|
| `record_received` | A supported record entered object processing. |
| `invalid_event` | A record failed source, event-name, shape, or key validation. |
| `object_identity_mismatch` | Event identity and current object metadata disagree. |
| `duplicate_skipped` | An existing valid completion marker was verified. |
| `processing_started` | Identity and body verification completed; transformation begins. |
| `processing_completed` | This invocation won completion publication. |
| `processing_failed` | Object processing failed before valid completion. |
| `completion_race_lost` | Conditional creation reported another completion writer. |
| `completion_race_validated` | The winning marker and its outputs passed validation. |

Logs never include raw CSV bodies, full quarantine rows, credentials, or environment
contents. Error messages are type-labelled and bounded to 500 characters.

## Metrics

Metrics use CloudWatch Embedded Metric Format in the same structured log stream, so the
adapter makes no direct CloudWatch API call. The namespace is `MetroPulse/DataPipeline`.
Only `Environment` and `PipelineVersion` are dimensions. Object key, input ID, request
ID, bucket, source date, and sequencer remain log fields rather than metric dimensions.

| Metric | Unit | Emission semantics |
|---|---|---|
| `ObjectsProcessed` | Count | One for the invocation that creates valid completion. |
| `ObjectsSkippedDuplicate` | Count | One for a verified existing completion or validated race winner. |
| `ObjectsFailed` | Count | One for each failed processing attempt. |
| `InputRows` | Count | Terminal input rows from the completion winner. |
| `ValidRows` | Count | Terminal valid rows from the completion winner. |
| `QuarantineRows` | Count | Terminal rejected rows from the completion winner. |
| `QuarantineRate` | Percent | `quarantine rows / input rows * 100` from the winner. |
| `ReconciliationFailures` | Count | One when the failed attempt reports reconciliation failure, otherwise zero. |
| `ProcessingDurationMs` | Milliseconds | Adapter duration from an injected monotonic clock. |

Success and row-count metrics are emitted only by the conditional completion winner.
Duplicates emit only `ObjectsSkippedDuplicate`; failures emit attempt-level failure and
duration metrics. CloudWatch Logs and Lambda delivery remain at least once, so a crash
after emitting a log or metric can still duplicate that operational record. Conditional
completion prevents practical double counting of terminal business metrics, but it is
not an exactly-once metrics system.

Per-object sampling warnings describe only timestamps visible in that raw object.
Cross-partition gaps, missing dates, overlaps, and reversed boundaries belong to the
scheduled/global audit, not individual validation invocations.

## Compaction and audit telemetry

Operations metrics use the same namespace with exactly `Environment` and
`ComponentVersion` as dimensions. Run ID, inventory ID, month, key, request ID, error
text, and rule ID remain structured log fields.

| Component | Metrics |
|---|---|
| Compaction | `CompactionRunsStarted`, `CompactionRunsSucceeded`, `CompactionRunsFailed`, `CompactionRunsNoOp`, `SelectedDays`, `CompactionInputRows`, `CompactionOutputRows`, `CompactionReconciliationFailures`, `ImmutableOutputConflicts`, `PublicationConflicts`, `CompactionDurationMs` |
| Audit | `AuditRunsSucceeded`, `AuditRunsFailed`, `MonthsInspected`, `MissingPublications`, `PublicationDrift`, `AuditRowReconciliationFailures`, `CrossMonthSignificantGaps`, `CrossMonthOverlaps`, `CrossMonthReversedBoundaries`, `AuditDurationMs` |

Compaction output counts are emitted only for a new publication. A fully verified retry
emits a no-op, not a second publication. Audit gap and coverage metrics are observations;
the verified 331 significant gaps do not increment an operational failure metric.

Alarms cover Lambda errors/throttles, reconciliation failures, immutable/publication
conflicts, missing approved publications, publication drift, and validation quarantine
rate. Missing data is non-breaching for these event-driven failure metrics. There is no
heartbeat alarm: CloudWatch limits the relevant evaluation window to seven days, which
cannot reliably represent a weekly schedule plus delivery and ingestion grace.
