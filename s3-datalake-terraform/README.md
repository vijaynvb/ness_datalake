# S3 Data Lake — Terraform Module

Provisions independent S3 buckets for a **medallion-architecture data lake**
(Silver, Gold, …), matching the structure described in the AWS S3 & Data
Partitioning session — but as **separate buckets per layer** rather than
prefixes within one bucket:

```bash
s3://deb-01-silver-layer-lab/            # raw data, minimal transformation
└── transport/bookings/year=2024/month=12/day=02/hour=00/bookings.parquet

s3://deb-01-gold-layer-lab/              # curated, business-ready data
└── datawarehouse/fact_bookings/year=2024/month=12/day=02/data.parquet
```

Partition keys (`year=`, `month=`, `day=`, `hour=`) are just object key
prefixes within each bucket — Terraform does not need to know about them;
your ingestion / transformation pipelines write objects using that key
convention when they `PutObject`.

Buckets are driven by the `layer_bucket_names` map, so adding a Data Mart
(or any other) layer as its own bucket is a one-line change — see
[Adding another layer](#adding-another-layer).

## What this creates

For each entry in `layer_bucket_names` (default: `silver`, `gold`):

- A private S3 bucket with the exact name you specify
- Bucket versioning
- Server-side encryption (SSE-S3 by default, or SSE-KMS if you supply a key)
- Full public access block
- Bucket-owner-enforced ownership controls (disables ACLs)
- Lifecycle rules:
  - `silver` bucket → transitions to STANDARD_IA then GLACIER over time (raw
    data is rarely reprocessed once curated)
  - `gold` bucket → optional Intelligent-Tiering
  - Noncurrent version expiration
  - Abort incomplete multipart uploads after 7 days

Plus, optionally, one shared access-logging bucket for all layer buckets.

Additionally, `layer_partition_prefixes` pre-creates zero-byte "folder
marker" objects for every intermediate level of the given partition
paths, so the full folder chain is visible immediately in the console —
by default:

```text
silver/transport/
silver/transport/bookings/
silver/transport/bookings/year=2024/
silver/transport/bookings/year=2024/month=12/
silver/transport/bookings/year=2024/month=12/day=02/
silver/transport/bookings/year=2024/month=12/day=02/hour=00/

gold/datawarehouse/
gold/datawarehouse/fact_bookings/
gold/datawarehouse/fact_bookings/year=2024/
gold/datawarehouse/fact_bookings/year=2024/month=12/
gold/datawarehouse/fact_bookings/year=2024/month=12/day=02/
```

(The `silver/` / `gold/` shown above is the bucket itself — each layer is
its own bucket, so these are keys *within* that bucket.) These are cosmetic
markers only; your actual `bookings.parquet` / `data.parquet` files are
written by your ingestion/transformation pipeline, not by Terraform.

## Execution steps

### 1. Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) `>= 1.5.0`
- AWS credentials with permission to manage S3 buckets, available via one of:
  - `aws configure` (uses `~/.aws/credentials`)
  - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (/ `AWS_SESSION_TOKEN`) env vars
  - An assumed role / SSO profile (`AWS_PROFILE=<profile>`)
- Verify credentials resolve before running Terraform:

  ```bash
  aws sts get-caller-identity
  ```

### 2. Configure variables

```bash
cd terraform-s3-datalake
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

- Set `aws_region`, `project_name`, `environment`.
- Change `layer_bucket_names` to bucket names that are **globally unique
  across all of AWS** (e.g. suffix with your account ID, as in the example
  file).
- Adjust `layer_partition_prefixes`, lifecycle days, `kms_key_arn`, etc. as
  needed — see [Key variables](#key-variables) below.

### 3. Initialize

Downloads the AWS provider and sets up the local backend:

```bash
terraform init
```

### 4. Review the plan

```bash
terraform plan
```

Confirm it shows only the resources you expect to create (buckets,
versioning, encryption, public access block, lifecycle rules, and — if
`enable_access_logging = true` — the shared logging bucket).

### 5. Apply

```bash
terraform apply
```

Type `yes` when prompted. On success, Terraform prints the
[outputs](#outputs) (bucket names, ARNs, layer URIs).

### 6. Verify

```bash
terraform output
aws s3 ls | grep <project_name>
```

Check the console or `aws s3api list-objects-v2 --bucket <bucket-name>` to
confirm the zero-byte partition folder markers were created.

### 7. Tear down (optional)

```bash
terraform destroy
```

Buckets with `force_destroy = false` (the default) will fail to delete if
they still contain objects — empty them first (`aws s3 rm s3://<bucket> --recursive`)
or set `force_destroy = true` before destroying.

## Key variables

| Variable | Default | Description |
|---|---|---|
| `layer_bucket_names` | `{ silver = "deb-01-silver-layer-lab", gold = "deb-01-gold-layer-lab" }` | Map of layer key → bucket name; one bucket per entry |
| `layer_partition_prefixes` | see [variables.tf](variables.tf) | Map of layer key → list of full partition paths to pre-create as folder markers (every intermediate level included) |
| `enable_versioning` | `true` | Bucket versioning |
| `kms_key_arn` | `""` | Set to use SSE-KMS instead of SSE-S3 |
| `silver_lifecycle_days_to_ia` / `_glacier` | `30` / `90` | Silver bucket storage tiering |
| `enable_intelligent_tiering` | `true` | Applies to the `gold` bucket |
| `block_public_access` | `true` | Keep `true` for a private data lake |
| `enable_access_logging` | `false` | Creates a shared `<project>-<env>-access-logs` bucket |

See [variables.tf](variables.tf) for the full list.

## Outputs

After `apply`:

- `bucket_names`, `bucket_arns` — maps keyed by layer
- `silver_bucket_name`, `silver_bucket_arn`, `silver_layer_uri`
- `gold_bucket_name`, `gold_bucket_arn`, `gold_layer_uri`

## Adding another layer

To provision a Data Mart bucket alongside silver/gold, add a key to
`layer_bucket_names` in your `terraform.tfvars`:

```hcl
layer_bucket_names = {
  silver   = "deb-01-silver-layer-lab"
  gold     = "deb-01-gold-layer-lab"
  datamart = "deb-01-datamart-layer-lab"
}
```

The datamart bucket will get the same baseline settings (versioning,
encryption, public access block, noncurrent-version expiration). It won't
automatically get the silver tiering rule or the gold Intelligent-Tiering
rule since those are keyed to `"silver"`/`"gold"` specifically — extend the
`dynamic "rule"` blocks in [main.tf](main.tf) if you want lifecycle rules
for it too.

## Notes

- **Prefixes vs. folders**: within each bucket, S3 has no real directories.
  `transport/bookings/...` is just a prefix within the object key.
- **Partitioning pattern**: your pipelines should write objects like
  `transport/bookings/year=2024/month=12/day=02/hour=00/bookings.parquet`
  inside the silver bucket. This Hive-style `key=value` partitioning is what
  lets Athena/Glue/Spark prune partitions and scan less data.
- Bucket names must be **globally unique across all of AWS** — if
  `deb-01-silver-layer-lab` / `deb-01-gold-layer-lab` are already taken,
  `apply` will fail with `BucketAlreadyExists`; pick different names.
- This module intentionally does not create Glue Catalog databases/crawlers,
  IAM roles, or compute (Glue jobs, EMR, Lambda) — it's scoped to the
  storage layer. Ask if you'd like those added.
