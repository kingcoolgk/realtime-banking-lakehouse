output "s3_bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lakehouse.name
}

output "glue_job_name" {
  value = aws_glue_job.curate_transactions.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}

output "redshift_workgroup_endpoint" {
  value = try(aws_redshiftserverless_workgroup.lakehouse[0].endpoint, "Redshift not enabled — set enable_redshift=true in terraform.tfvars and re-apply")
  sensitive = true
}

output "redshift_admin_username" {
  value = try(aws_redshiftserverless_namespace.lakehouse[0].admin_username, "Redshift not enabled")
  sensitive = true
}
