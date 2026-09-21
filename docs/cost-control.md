# Cost controls

The repository creates definitions only, so there is no deployed AWS cost evidence yet. Future
cost review should use the Terraform plan, service calculators, billing tools, and actual
usage rather than an exact price claim.

- ECR uses immutable tags, scan-on-push, seven-day expiry for untagged images, and retains
  only five tagged `build-` images.
- Staging expires after 90 days by default and quarantine after 365 days. Current
  `control/` evidence remains retained while current curated data remains retained;
  superseded control versions expire after 90 days. Raw and curated current objects
  remain durable, while their noncurrent versions expire after 365 days.
- Athena results expire after 30 days in a separate bucket. The Phase 5 workgroup enforces
  its result prefix and a configurable 268,435,456-byte (256 MiB) default per-query scan
  cutoff. Routine SQL filters `year` and `month` and selects explicit columns.
- CloudWatch validation, compaction, and audit logs retain 14 days by default.
- Validation uses a small reserved-concurrency default of two. Compaction and audit each
  use one reserved execution, bounded five-minute timeouts, and no provisioned concurrency.
- The architecture has no VPC, NAT Gateway, always-running compute, or development
  ingestion schedule. The weekly full-checksum audit schedule is disabled by default.
- `Project`, `Environment`, `ManagedBy`, `Owner`, and `Purpose` tags support resource and
  cost inventory.

Retention and runtime settings are variables so a reviewed environment can adjust them.
The dev-only force-delete variables default to false and are teardown controls, not
routine cost-management switches.

Monthly compaction reduces 212 daily staging files to 8 curated files. These files remain
below generic large-lake sizing guidance, but this proportionate reduction cuts request
and metadata overhead without adding distributed compute. Superseded immutable runs need
a reviewed lifecycle only after the currently published Glue location and retention
requirements are known; Phase 6 does not delete them.

A weekly full-checksum audit reads the current curated files (about 39.4 MiB for the
verified local dataset) plus control evidence. This is intentionally stronger than HEAD
metadata and has a small, bounded S3/Lambda request and transfer cost for this portfolio.
Known data gaps do not generate operational retries or alarms.
