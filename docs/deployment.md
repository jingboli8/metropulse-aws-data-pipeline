# Local packaging and future deployment runbook

Phase 4 defines infrastructure and container packaging. Nothing in this runbook has been
applied to AWS, and no image has been pushed. Commands that create AWS resources are for
a later explicitly authorized development-account session.

## Verify the container locally

From the repository root in PowerShell, run exactly:

```powershell
.\scripts\verify_lambda_container.ps1
```

The script runs `docker buildx build` with `--platform linux/amd64 --load`, then verifies
that the exact `metropulse-lambda:phase4-local` tag exists in the local image store before
inspection. It stops on the first failed command or assertion. A successful run ends
with output shaped like this; image identifiers and sizes vary:

```text
PYARROW_OK version=21.0.0
VALIDATION_HANDLER_IMPORT_OK
COMPACTION_HANDLER_IMPORT_OK
AUDIT_HANDLER_IMPORT_OK
SMOKE_OK rows=1 quarantine=0 codec=SNAPPY identity=<sha256>
OPERATIONS_SMOKE_OK rows=2 gaps=1 inventory=<sha256> run_id=<sha256>
TASK_CONTENT_OK
VALIDATION_HANDLER_CONFIG_OK metropulse.aws.lambda_handler.lambda_handler
COMPACTION_HANDLER_CONFIG_OK metropulse.aws.compaction_lambda_handler.lambda_handler
AUDIT_HANDLER_CONFIG_OK metropulse.aws.audit_lambda_handler.lambda_handler
LAMBDA_DEFAULT_USER_CONFIG_OK
HISTORY_SECRET_SCAN_OK
IMAGE_ID=sha256:<digest>
IMAGE_SIZE_BYTES=<bytes>
CONTAINER_VERIFICATION_OK
```

The verifier checks all three handler imports and commands, runs validation plus
deterministic compaction/idempotent-retry/stale-claim/audit smoke tests against in-memory
storage, inspects `/var/task`, scans text content for
Windows absolute paths in repository-owned runtime code, and checks image history for
common secret-bearing arguments. The whole task tree still receives dependency-inventory,
cache, compiled-file, test, generated-data, and project-file checks. Third-party PyArrow
source is outside the first-party content-leakage scope. The content audit uses a mounted
Python standard-library helper because the minimal AL2023 image does not provide `find`.
It makes no AWS call and performs no push.

The user executed the final verifier externally on 2026-09-16. PyArrow 21.0.0 imported,
the handler imported and matched the configured command, and the deterministic smoke
test produced one valid row, zero quarantine rows, Snappy Parquet, and processing
identity `65194c3ac9faac266a6fcd75970a10dad1fa8f21c1bb7abd958b2ffd3fe7979c`.
The task audit inspected 740 entries. The default-user configuration and Docker-history
secret scan passed. The loaded image was 233,356,828 bytes (approximately 222.5 MiB).
This is local packaging evidence supplied by the user, not AWS deployment evidence. A
local image ID was also reported, but it is not a durable reproducibility identifier;
BuildKit provenance or attestation output can change it between builds. The pinned base
image digest and hash-pinned dependency are the durable build inputs.

The user subsequently ran the updated Phase 7 verifier externally. PyArrow 21.0.0 and
the imports and configured commands for the validation, compaction, and audit handlers
passed. The validation smoke produced one valid row, zero quarantine rows, and Snappy
Parquet. The operations smoke reconciled two rows and one timestamp gap. Its reported
inventory and run IDs are deterministic synthetic fixture identities, not production
data. The task audit inspected 762 entries; the Lambda default-user configuration and
Docker-history secret scan passed. The loaded image was 233,386,762 bytes
(approximately 222.6 MiB). The reported local image ID is not recorded as a durable
deployment digest because BuildKit provenance or attestation can change it; the pinned
base-image digest, hash-pinned dependency, and eventual ECR digest provide the relevant
build and deployment identities.

## Validate Terraform without AWS

The four directories are independent root stacks and use local state if later applied.
Initialization downloads only the signed HashiCorp AWS provider; native tests use a mock
provider.

```powershell
terraform -chdir=infra/bootstrap init -backend=false
terraform -chdir=infra/bootstrap validate
terraform -chdir=infra/bootstrap test
terraform -chdir=infra/platform init -backend=false
terraform -chdir=infra/platform validate
terraform -chdir=infra/platform test
terraform -chdir=infra/query init -backend=false
terraform -chdir=infra/query validate
terraform -chdir=infra/query test
terraform -chdir=infra/operations init -backend=false
terraform -chdir=infra/operations validate
terraform -chdir=infra/operations test
terraform fmt -check -recursive infra
```

Do not run `plan` or `apply` merely to validate this repository. No remote-state backend
is configured. `.terraform/`, plans, overrides, variable files, crash logs, and state are
ignored; each root's signed dependency lock file is tracked.

The query and operations roots consume explicit inputs rather than remote state. The query root's future
apply follows `infra/platform` and also requires the reviewed 12-digit expected owner of
the results bucket. Phase 5 creates an empty table contract only: do not run Athena SQL
until Phase 6 has published and explicitly registered approved curated partitions. The
operations root follows platform and query. Keep its audit schedule disabled until every
curated partition and the exact immutable inventory reference have been reviewed.

## Future bootstrap and release sequence

When an AWS development account is explicitly authorized:

1. Choose unique bucket names, the Region, and reviewed tag values outside source code.
2. Apply `infra/bootstrap` to create the same-Region immutable ECR repository.
3. Build the verified Linux amd64 image and tag it `build-<revision>`.
4. Authenticate Docker to that ECR repository, push the image, and read its registry
   digest.
5. Form `repository-url@sha256:<digest>`. Configure the `infra/platform`
   `lambda_image_uri` with that value; tag-only URIs fail validation.
6. Review a saved platform plan before any apply. Confirm the two exact bucket names,
   image digest, IAM policy, tags, notification filter, lifecycle rules, and force-delete
   values.
7. Apply the platform only after review. The raw notification accepts only
   `raw/source=metropt3/` keys ending in `data.csv`; staging, quarantine, and control
   writes cannot match it.
8. Apply the query layer before operations so the catalog identity exists. Invoke
   compaction only with an exact approved selection. Publish and pin an audit inventory,
   then enable the weekly audit schedule in a separately reviewed operations plan.

ECR and Lambda must use the same AWS Region. Rebuilding never changes an existing
digest. Push a newly tagged image, capture the new digest, and update the platform input
to release application or dependency changes.

## Recovery and teardown

S3 notifications are at-least-once and unordered. Phase 3 completion markers and
deterministic outputs handle duplicate delivery, concurrent claims, and retry after
partial output. The notification itself does not provide ordering.

Before destroy, inventory resources by `Project` and `Environment` tags, disable event
sources, and back up any raw, curated, quarantine, or control data that must survive.
Destroy the platform first. Buckets default to `force_destroy = false`, so a nonempty
bucket blocks accidental deletion. Set the dev-only escape hatch only after reviewing
and deliberately emptying the named project buckets. Remove ECR images deliberately,
destroy `infra/bootstrap` last, and leave its `allow_force_delete` false unless the exact
repository contents have been reviewed. Never use broad account-wide empty/delete
commands.

## Monthly publication recovery

Monthly publication writes and verifies the run-specific Parquet object, conditionally
writes completion, acquires and rechecks the strict month claim, then creates/updates and
reads back Glue. A stale month-publication claim is never automatically taken over. A
failed replacement preserves the old approved partition. `published.json` is historical
evidence rather than authority for the current catalog location. The local Phase 6
acceptance does not create Glue state.
