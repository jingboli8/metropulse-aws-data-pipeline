# ADR 001: Split the source locally before event-driven ingestion

- **Status:** Accepted
- **Date:** 2026-09-10

## Context

UCI distributes MetroPT-3 as one ZIP containing a 218,300,507-byte CSV with 1,516,948
rows. The data spans 212 source dates. A typical day is about 7,435 rows and the largest
observed day is 8,717 rows. Repeatedly scanning or processing the monolithic CSV in Lambda
would obscure the unit of retry and add avoidable runtime, memory, and transfer risk.

## Decision

A reproducible local backfill CLI is the only component that downloads the official UCI
ZIP. It verifies the recorded SHA-256, safely extracts it, streams the CSV, and writes one
raw CSV object per derived source date before upload. It also uploads an immutable source
archive to landing for provenance when an AWS environment is explicitly in use.

Daily raw keys are deterministic:

```text
raw/source=uci-metropt3/source_date=YYYY-MM-DD/metropt3-YYYY-MM-DD.csv
```

The splitter preserves source values and source headers, including the blank first header
and `DV_eletric`. Each daily manifest links back to the ZIP checksum. S3 `ObjectCreated`
notifications invoke validation only when the key begins `raw/` and ends `.csv`.

AWS runtime components never fetch from UCI and never split the monolithic CSV. Core
split/validation logic is independent of AWS SDK and Lambda event objects; CLI, Lambda,
and S3 behaviors remain thin adapters.

## Consequences

- Daily inputs are small, understandable validation and retry boundaries.
- Raw daily CSV remains replayable, while the source ZIP preserves original provenance.
- The local backfill is a deliberate operational step rather than a fully cloud-native
  acquisition service.
- The CLI needs restart-safe streaming, deterministic manifests, and exact row
  reconciliation. Raw and generated files must remain outside Git.
