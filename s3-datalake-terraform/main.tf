data "aws_caller_identity" "current" {}

locals {
  common_tags = merge(
    {
      Layer = "data-lake"
    },
    var.tags
  )

  # Append the current AWS account ID to each configured bucket name prefix
  # so bucket names stay globally unique per account without hardcoding it.
  layer_bucket_names = {
    for layer, name in var.layer_bucket_names :
    layer => "${name}-${data.aws_caller_identity.current.account_id}"
  }

  # Expand each configured partition path into every intermediate prefix,
  # so a single deep path (e.g. "a/b/c/") also creates "a/" and "a/b/".
  # Result: list of { layer = "silver", key = "transport/" } objects, one
  # per intermediate level per configured path, deduped per layer.
  partition_prefix_pairs = flatten([
    for layer, paths in var.layer_partition_prefixes : [
      for path in paths : [
        for i in range(length(split("/", trimsuffix(path, "/")))) : {
          layer = layer
          key   = "${join("/", slice(split("/", trimsuffix(path, "/")), 0, i + 1))}/"
        }
      ]
    ]
  ])

  # Dedupe by "layer|key" (multiple configured paths can share intermediate
  # prefixes, e.g. two silver paths both under "transport/bookings/").
  partition_markers = {
    for pair in local.partition_prefix_pairs :
    "${pair.layer}|${pair.key}" => pair
  }
}

# ---------------------------------------------------------------------------
# One independent bucket per medallion layer (silver = raw, gold = curated).
# Add more entries to var.layer_bucket_names (e.g. datamart = "...") to
# provision additional layer buckets with the same settings.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "layer" {
  for_each = local.layer_bucket_names

  bucket        = each.value
  force_destroy = var.force_destroy

  tags = merge(local.common_tags, {
    Name  = each.value
    Layer = each.key
  })
}

resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_arn != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
    }
    bucket_key_enabled = var.kms_key_arn != "" ? true : null
  }
}

resource "aws_s3_bucket_public_access_block" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id

  block_public_acls       = var.block_public_access
  block_public_policy     = var.block_public_access
  ignore_public_acls      = var.block_public_access
  restrict_public_buckets = var.block_public_access
}

resource "aws_s3_bucket_ownership_controls" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# ---------------------------------------------------------------------------
# Zero-byte "folder marker" objects for the configured partition paths, plus
# every intermediate level, so the full folder chain is visible immediately
# in the console. S3 has no real folders — these are cosmetic prefix markers.
#   silver: transport/, transport/bookings/, .../year=2024/, .../month=12/,
#           .../day=02/, .../hour=00/
#   gold:   datawarehouse/, .../fact_bookings/, .../year=2024/, .../month=12/,
#           .../day=02/
# ---------------------------------------------------------------------------
resource "aws_s3_object" "partition_markers" {
  for_each = local.partition_markers

  bucket       = aws_s3_bucket.layer[each.value.layer].id
  key          = each.value.key
  content_type = "application/x-directory"
  content      = "" # zero-byte marker object; content is irrelevant

  depends_on = [aws_s3_bucket_ownership_controls.layer]
}

# ---------------------------------------------------------------------------
# Lifecycle rules per layer
#   - silver bucket (raw): transitions to cheaper storage over time since raw
#     data is reprocessed rarely once curated into gold.
#   - gold bucket (curated): optionally uses Intelligent-Tiering since BI
#     access patterns are less predictable.
#   - All layer buckets: expire noncurrent versions + abort stale uploads.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket_lifecycle_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id

  # Required when versioning is enabled, per provider constraints.
  depends_on = [aws_s3_bucket_versioning.layer]

  dynamic "rule" {
    for_each = each.key == "silver" && (var.silver_lifecycle_days_to_ia > 0 || var.silver_lifecycle_days_to_glacier > 0 || var.silver_lifecycle_expiration_days > 0) ? [1] : []
    content {
      id     = "silver-raw-layer-tiering"
      status = "Enabled"

      filter {}

      dynamic "transition" {
        for_each = var.silver_lifecycle_days_to_ia > 0 ? [1] : []
        content {
          days          = var.silver_lifecycle_days_to_ia
          storage_class = "STANDARD_IA"
        }
      }

      dynamic "transition" {
        for_each = var.silver_lifecycle_days_to_glacier > 0 ? [1] : []
        content {
          days          = var.silver_lifecycle_days_to_glacier
          storage_class = "GLACIER"
        }
      }

      dynamic "expiration" {
        for_each = var.silver_lifecycle_expiration_days > 0 ? [1] : []
        content {
          days = var.silver_lifecycle_expiration_days
        }
      }
    }
  }

  dynamic "rule" {
    for_each = each.key == "gold" && var.enable_intelligent_tiering ? [1] : []
    content {
      id     = "gold-intelligent-tiering"
      status = "Enabled"

      filter {}

      transition {
        days          = 0
        storage_class = "INTELLIGENT_TIERING"
      }
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = var.enable_versioning ? "Enabled" : "Disabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ---------------------------------------------------------------------------
# Optional server access logging bucket (shared target for all layer buckets)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "logs" {
  count = var.enable_access_logging ? 1 : 0

  bucket        = "${var.project_name}-${var.environment}-access-logs"
  force_destroy = var.force_destroy

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-${var.environment}-access-logs"
  })
}

resource "aws_s3_bucket_ownership_controls" "logs" {
  count  = var.enable_access_logging ? 1 : 0
  bucket = aws_s3_bucket.logs[0].id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  count  = var.enable_access_logging ? 1 : 0
  bucket = aws_s3_bucket.logs[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "layer" {
  for_each = var.enable_access_logging ? aws_s3_bucket.layer : {}

  bucket        = each.value.id
  target_bucket = aws_s3_bucket.logs[0].id
  target_prefix = "s3-access-logs/${each.key}/"
}
