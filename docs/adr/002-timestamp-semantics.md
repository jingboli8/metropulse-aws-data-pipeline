# ADR 002: Preserve source wall-clock timestamps with unknown timezone

- **Status:** Accepted
- **Date:** 2026-09-10

## Context

MetroPT-3 timestamps have second precision but no offset or timezone. UCI does not provide
enough information to identify UTC, a local zone, or daylight-saving behavior. Assigning
a timezone would invent meaning and could shift daily/monthly partition boundaries.
Observed data is approximately 0.1 Hz, mostly at 10-second intervals, despite conflicting
upstream 1 Hz wording. Significant collection gaps exist.

## Decision

Parse the source value without adjustment and name it `event_timestamp_local`. Store it
as Arrow `timestamp[ms]` without timezone, Parquet timestamp with
`isAdjustedToUTC=false`, and Athena `timestamp`. Record
`event_timestamp_timezone_status = 'unknown'` in data plus manifests/schema metadata.

Derive `source_date` directly from that unshifted wall-clock value only for raw routing,
partitioning, and reconciliation. Derive curated `year` and `month` from `source_date`.
Do not add `Z`, convert to UTC, infer a named zone, or invent daylight-saving rules.

Sampling intervals and gaps are warnings/metrics. The documented significant-gap
threshold is greater than 60 seconds. No missing rows are imputed, and valid rows around
a gap are not quarantined.

## Consequences

- Queries must understand that `event_timestamp_local` is not globally comparable to an
  offset-aware time without new authoritative metadata.
- Partition dates reproduce the source calendar consistently.
- A future authoritative timezone would require a new explicit field and versioned
  migration; it would not silently reinterpret stored values.
