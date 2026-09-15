output "bucket_names" {
  description = "Map of layer -> S3 bucket name."
  value       = { for k, b in aws_s3_bucket.layer : k => b.id }
}

output "bucket_arns" {
  description = "Map of layer -> S3 bucket ARN."
  value       = { for k, b in aws_s3_bucket.layer : k => b.arn }
}

output "silver_bucket_name" {
  description = "Name of the Silver (raw) layer bucket."
  value       = try(aws_s3_bucket.layer["silver"].id, null)
}

output "silver_bucket_arn" {
  description = "ARN of the Silver (raw) layer bucket."
  value       = try(aws_s3_bucket.layer["silver"].arn, null)
}

output "gold_bucket_name" {
  description = "Name of the Gold (curated) layer bucket."
  value       = try(aws_s3_bucket.layer["gold"].id, null)
}

output "gold_bucket_arn" {
  description = "ARN of the Gold (curated) layer bucket."
  value       = try(aws_s3_bucket.layer["gold"].arn, null)
}

output "silver_layer_uri" {
  description = "Example S3 URI prefix for Silver raw data, e.g. transport/bookings/year=2024/month=06/day=02/hour=00/"
  value       = try("s3://${aws_s3_bucket.layer["silver"].id}/", null)
}

output "gold_layer_uri" {
  description = "Example S3 URI prefix for Gold curated data, e.g. datawarehouse/fact_bookings/year=2024/month=06/day=02/"
  value       = try("s3://${aws_s3_bucket.layer["gold"].id}/", null)
}

output "logging_bucket_name" {
  description = "Name of the shared server access logging bucket, if enabled."
  value       = var.enable_access_logging ? aws_s3_bucket.logs[0].id : null
}

output "source_file_uris" {
  description = "Map of upload key -> full S3 URI for each uploaded source file."
  value = {
    for k, o in aws_s3_object.source_files :
    k => "s3://${aws_s3_bucket.layer[var.source_files[k].layer].id}/${o.key}"
  }
}
