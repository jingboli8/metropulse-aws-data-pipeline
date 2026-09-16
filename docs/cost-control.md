# Cost controls

Phase 4 creates definitions only, so there is no deployed AWS cost evidence yet. Future
cost review should use the Terraform plan, service calculators, billing tools, and actual
usage rather than an exact price claim.

- ECR uses immutable tags, scan-on-push, seven-day expiry for untagged images, and retains
  only five tagged `build-` images.
- Staging expires after 90 days by default; quarantine after 365 days; control records
  after 730 days. Noncurrent versions have bounded retention. Raw and curated current
  objects remain durable, while their noncurrent versions expire after 365 days.
- Athena results expire after 30 days in a separate bucket. The Phase 5 workgroup enforces
  its result prefix and a configurable 268,435,456-byte (256 MiB) default per-query scan
  cutoff. Routine SQL filters `year` and `month` and selects explicit columns.
- CloudWatch validation logs retain 14 days by default.
- Lambda uses a small reserved-concurrency default of two, a bounded five-minute timeout,
  and no provisioned concurrency.
- The architecture has no VPC, NAT Gateway, always-running compute, or development
  schedule. EventBridge schedules remain a later phase and disabled by default in dev.
- `Project`, `Environment`, `ManagedBy`, `Owner`, and `Purpose` tags support resource and
  cost inventory.

Retention and runtime settings are variables so a reviewed environment can adjust them.
The dev-only force-delete variables default to false and are teardown controls, not
routine cost-management switches.
