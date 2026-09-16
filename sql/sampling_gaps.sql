-- Intentional complete-history scan: removing month boundaries would hide cross-month gaps.
-- event_timestamp_local is a timezone-unknown source wall-clock value. No rows are imputed.
WITH ordered_observations AS (
    SELECT
        record_index,
        event_timestamp_local,
        source_date,
        lag(event_timestamp_local) OVER (
            ORDER BY event_timestamp_local, record_index
        ) AS previous_event_timestamp_local
    FROM metropulse_dev.metropt3_curated
),
observed_intervals AS (
    SELECT
        record_index,
        source_date,
        previous_event_timestamp_local,
        event_timestamp_local,
        date_diff(
            'second',
            previous_event_timestamp_local,
            event_timestamp_local
        ) AS interval_seconds
    FROM ordered_observations
    WHERE previous_event_timestamp_local IS NOT NULL
)
SELECT
    record_index,
    source_date,
    previous_event_timestamp_local,
    event_timestamp_local,
    interval_seconds
FROM observed_intervals
WHERE interval_seconds > 60
ORDER BY event_timestamp_local, record_index;
