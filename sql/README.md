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
artifacts and may receive a separate operational interface in a later phase.
