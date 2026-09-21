# Security controls

## Storage and transport

Both defined S3 buckets block all public-access mechanisms, enforce bucket-owner
ownership, and enable default SSE-S3 encryption. Explicit bucket-policy denies reject
all non-TLS requests. The `s3:*` action and wildcard principal in those **Deny-only**
transport statements are required to cover every S3 operation; they grant no access.
The data lake enables versioning. No public ACL, website, CORS rule, or empty folder
object is defined.

Bucket names, AWS account ID, image URI, and deployment-specific values remain Terraform
inputs. Source code contains no credentials, KMS key IDs, personal bucket names, or
account-specific values. Encryption uses AWS-managed SSE-S3 so this phase does not
hard-code an account-specific KMS key.

## Lambda identity and scope

The runtime role can read and head objects only through `s3:GetObject` and
`s3:GetObjectVersion` under `raw/source=metropt3/`. It can read/head and conditionally put
objects only under the documented staging, quarantine, and validation-control prefixes.
S3 conditional writes use the same `s3:PutObject` permission as ordinary object puts.
The role can create streams and put events only in the pre-created validation log group.
It has no ECR write, IAM administration, bucket deletion, Glue, Athena, EventBridge,
DynamoDB, SQS, Kinesis, Redshift, or wildcard Allow permission.

The ECR repository policy grants the Lambda service principal only
`ecr:BatchGetImage` and `ecr:GetDownloadUrlForLayer`, which are required to retrieve the
private image. These permissions are not attached to the function's runtime role.

Lambda invocation permission is limited to the data-lake bucket ARN. The S3 notification
depends on that permission and filters exactly to `raw/source=metropt3/` plus `data.csv`,
so generated staging, quarantine, and control keys cannot recursively invoke the
function.

## Query-layer boundary

The Phase 5 Glue table points only to `curated/metropt3/`; it does not expose raw,
staging, quarantine, or control objects. Partition projection and crawlers are disabled,
and no partitions exist until Phase 6 approves an immutable monthly run. The Athena
workgroup overrides client settings with the isolated results prefix, expected bucket
owner, SSE-S3 encryption, and a 256 MiB query cutoff.

Phase 5 adds no analyst role, compactor role, or permission to the validation Lambda.
Future query principals should receive only workgroup execution, read-only catalog
metadata, curated-object reads, and access to the one query-result prefix. Those IAM
policies require a separately reviewed deployment phase.

## Supply chain and runtime

The Docker base uses an official AWS Lambda Python 3.12 Linux amd64 image manifest
digest. PyArrow 21.0.0 uses a hash-verified CPython 3.12 Linux amd64 wheel. The Docker
context is an allowlist containing only the Dockerfile, runtime requirement, production
package, and bounded standard-library cleanup helper. A multi-stage build copies only
the cleaned `/var/task` tree, so the helper and requirement input are absent from the
runtime image. The image does not include `.git`, `.venv`, tests, data, artifacts, local
environment files, or Terraform files. The AWS base supplies boto3; changing that SDK
requires an intentional base-digest update.

The final-image audit separates ownership scopes. The `/var/task` inventory requires the
`metropulse` package, PyArrow import package, and pinned-version distribution metadata.
The wheel-specific `pyarrow.libs` directory is allowed when present but is not required;
some compatible PyArrow 21.0.0 wheel layouts keep their native libraries inside the
import package. Any other top-level entry is rejected. The entire tree is checked for
escaping links, tests, caches, compiled bytecode, generated data, credentials files, and
packaged build inputs. Only repository-owned `metropulse/` text is scanned for Windows
or user-profile paths, repository-machine leakage, access-key patterns, secret
assignments, and private-key material. Generic path-handling or documentation literals
in pinned third-party PyArrow source are not treated as first-party leakage.

Lambda supplies its default least-privileged runtime user because the Dockerfile does
not set `USER`. The application is memory-first and uses `/tmp` as its only writable
temporary location. There is no VPC attachment, secret environment variable, or embedded
credential. Container history and contents are checked by the user-run verifier before
commit. The final externally executed verification passed the task-content,
default-user, and Docker-history secret checks.

## Review checklist

- Run Ruff, pytest, Terraform format/validate/mock tests, and the local container verifier.
- Review the digest-pinned image URI and signed provider lock files.
- Confirm execution IAM has no wildcard Allow actions or resources.
- Confirm force deletion remains false unless an exact dev teardown is approved.
- Scan tracked files for credentials, account IDs, personal buckets, machine paths,
  generated data, Terraform state, plans, and image archives.
- Never inspect local AWS credential files as part of this workflow.

## Operations roles

Phase 7 defines separate roles without applying them. The compactor can read exact
validation/compaction evidence and staging/curated objects, write only curated and
compaction-control prefixes, and call `glue:GetTable`, `glue:GetPartition`,
`glue:CreatePartition`, and `glue:UpdatePartition` for one catalog/database/table. The
auditor can only read audit/compaction control evidence, curated objects, and Glue table
or partition metadata. The Scheduler role may invoke only the audit Lambda.

None can list a bucket, execute Athena, delete a partition, write ECR, administer IAM,
or use wildcard Allow actions/resources. EMF travels through each function's restricted
log group and requires no `cloudwatch:PutMetricData`. The validation role is unchanged.
Current `control/` evidence does not expire while current curated objects are retained;
only noncurrent control versions have bounded lifecycle expiry.
