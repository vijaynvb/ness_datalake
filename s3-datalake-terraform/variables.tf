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

variable "layer_bucket_names" {
  description = <<-EOT
    Map of medallion layer -> S3 bucket name prefix. The current AWS account
    ID is automatically appended to each value (via data.aws_caller_identity)
    to keep bucket names globally unique.
    Example: { silver = "deb-01-silver-layer-lab", gold = "deb-01-gold-layer-lab" }
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
  default     = false
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
    Example:
      silver = ["transport/bookings/year=2024/month=12/day=02/hour=00/"]
      gold   = ["datawarehouse/fact_bookings/year=2024/month=12/day=02/"]
  EOT
  type        = map(list(string))
  default = {
    silver = [
      "transport/bookings/year=2024/month=12/day=02/hour=00/",
    ]
    gold = [
      "datawarehouse/fact_bookings/year=2024/month=12/day=02/",
    ]
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
