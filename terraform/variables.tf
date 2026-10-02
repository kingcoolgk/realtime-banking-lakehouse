variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Prefix used for naming all resources"
  type        = string
  default     = "banking-pipeline"
}

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for the lakehouse. Must be changed before apply — bucket names are unique across all of AWS."
  type        = string
  default     = "banking-pipeline-lakehouse-gkumar-2026"
}

variable "redshift_admin_password" {
  description = "Admin password for the Redshift Serverless namespace. Override via terraform.tfvars or TF_VAR_redshift_admin_password env var — do not commit a real value."
  type        = string
  sensitive   = true
  default     = "ChangeMe123!"
}

variable "redshift_base_rpu" {
  description = "Base capacity (RPUs) for Redshift Serverless. 8 is the minimum and keeps demo cost low."
  type        = number
  default     = 8
}

variable "enable_redshift" {
  description = "Whether to provision Redshift Serverless. Keep this false while building/testing everything else — S3, Glue, and Athena cost only cents. Flip to true only when you're ready to take Redshift screenshots, then flip back to false and re-apply to tear it down."
  type        = bool
  default     = false
}
