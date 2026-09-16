variable "aws_region" {
  description = "AWS region to deploy the data lake buckets into."
  type        = string
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "Short name used for tagging and default naming."
  type        = string
  default     = "datalake"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "layer_bucket_name_prefixes" {
  description = <<-EOT
    Map of medallion layer -> S3 bucket name prefix (without the account ID).
    Each key becomes an independent bucket (no shared prefixes). The current
    AWS account ID is appended automatically at apply time (via
    data.aws_caller_identity) so the resulting bucket name is globally unique,
    e.g. prefix "deb-01-silver-layer-lab" -> "deb-01-silver-layer-lab-123456789012".
  EOT
  type        = map(string)
  default = {
    silver = "deb-01-silver-layer-lab"
    gold   = "deb-01-gold-layer-lab"
  }
}

variable "force_destroy" {
  description = "If true, allows Terraform to delete a bucket even if it still contains objects. Use with caution (recommended false in prod)."
  type        = bool
  default     = true
}

variable "enable_versioning" {
  description = "Enable S3 bucket versioning."
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "Optional KMS key ARN for SSE-KMS encryption. If empty, SSE-S3 (AES256) is used instead."
  type        = string
  default     = ""
}

variable "silver_lifecycle_days_to_ia" {
  description = "Days after which Silver (raw) layer objects transition to STANDARD_IA. Set to 0 to disable."
  type        = number
  default     = 30
}

variable "silver_lifecycle_days_to_glacier" {
  description = "Days after which Silver (raw) layer objects transition to GLACIER. Set to 0 to disable."
  type        = number
  default     = 90
}

variable "silver_lifecycle_expiration_days" {
  description = "Days after which Silver (raw) layer objects expire/delete. Set to 0 to disable."
  type        = number
  default     = 0
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which noncurrent object versions are permanently deleted (requires versioning)."
  type        = number
  default     = 90
}

variable "layer_partition_prefixes" {
  description = <<-EOT
    Map of layer -> list of full partition prefixes (S3 key paths ending in
    "/") to pre-create as zero-byte "folder marker" objects in that layer's
    bucket. Every intermediate level of each path is also created, so a
    single deep path creates the full folder chain down to it.
    Default mirrors the folder structure from sparkLab/Docs/04_Datapipeline.md:
      silver = ["transport/bookings/year=2024/month=06/",
                "exchange-rates-monthly/year=2024/month=06/"]
      gold   = ["datawarehouse/staging_fact_bookings/year=2024/month=06/",
                "prod_artifacts/"]
  EOT
  type        = map(list(string))
  default = {
    silver = [
      "transport/bookings/year=2024/month=06/",
      "exchange-rates-monthly/year=2024/month=06/",
    ]
    gold = [
      "datawarehouse/staging_fact_bookings/year=2024/month=06/",
      "prod_artifacts/",
    ]
  }
}

variable "source_files" {
  description = <<-EOT
    Map of upload key -> source file upload config. Each entry uploads a
    local file (from the "source/" directory by default) into the given
    layer bucket at the given key. Used to push the actual lab datasets
    (taxi trips, exchange rates) and the PySpark pipeline script into S3,
    matching the paths described in sparkLab/Docs/04_Datapipeline.md.
  EOT
  type = map(object({
    layer       = string
    key         = string
    source_path = string
  }))
  default = {
    taxi_trips = {
      layer       = "silver"
      key         = "transport/bookings/year=2024/month=06/green_tripdata_2024-06.parquet"
      source_path = "source/green_tripdata_2024-06.parquet"
    }
    exchange_rates = {
      layer       = "silver"
      key         = "exchange-rates-monthly/year=2024/month=06/exchange_rates_2024_06.parquet"
      source_path = "source/exchange_rates_2024_06.parquet"
    }
    pipeline_script = {
      layer       = "gold"
      key         = "prod_artifacts/03-data-pipeline.py"
      source_path = "source/03-data-pipeline.py"
    }
  }
}

variable "enable_intelligent_tiering" {
  description = "Enable S3 Intelligent-Tiering for the Gold layer bucket (cost optimization for unpredictable access patterns)."
  type        = bool
  default     = true
}

variable "block_public_access" {
  description = "Block all public access to the buckets (recommended true for a private data lake)."
  type        = bool
  default     = true
}

variable "enable_access_logging" {
  description = "Enable S3 server access logging to a dedicated logging bucket."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags to apply to all resources."
  type        = map(string)
  default     = {}
}
