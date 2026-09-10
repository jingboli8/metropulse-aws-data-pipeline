# ADR 005: Normalize names and types while retaining source lineage

- **Status:** Accepted
- **Date:** 2026-09-10

## Context

The source CSV has 17 columns: an unnamed index, a timestamp, seven analogue signals, and
eight digital signals. Names mix case and underscores, and `DV_eletric` contains a source
spelling error. Digital fields are encoded as floating-looking `0.0` and `1.0`. Silently
changing names would weaken lineage; preserving source shapes in curated data would make
queries less consistent.

## Decision

Raw daily CSV preserves the exact source header. Staging and curated data use snake_case
names and explicit Arrow/Parquet/Athena types defined in the data contract:

- blank source header maps to `record_index` as 64-bit integer;
- `timestamp` maps to timezone-naive `event_timestamp_local`;
- seven analogue values map to 64-bit floating point;
- eight digital values map to Boolean only after exact `0`/`1` validation;
- `DV_eletric` maps to `dv_electric` downstream;
- derived `source_date` and timezone status are explicit.

The schema registry/mapping, Parquet metadata, and validation manifests retain each exact
source name. The correction does not rewrite raw files. `Caudal_impulses` becomes
`caudal_impulses` by case normalization only because no authoritative English rename is
available.

## Consequences

- Athena receives predictable names and useful Boolean types.
- Reviewers can trace every curated field to the exact original header, including the
  blank header and typo.
- Source drift fails validation instead of being silently normalized. Breaking name,
  type, or semantic changes require a major schema/pipeline version and explicit
  reprocessing.
