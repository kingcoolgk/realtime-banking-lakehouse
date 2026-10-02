terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# ── S3 Lakehouse ────────────────────────────────────────────────────────────
# Single bucket, three prefixes (raw / curated / dq_reports) — mirrors the
# existing local demo-mode layout in config/config.yaml.

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Glue Data Catalog ────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "lakehouse" {
  name        = replace("${var.project_name}_db", "-", "_")
  description = "Catalog database for the real-time banking lakehouse (raw + curated)."
}

resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.lakehouse.arn,
        "${aws_s3_bucket.lakehouse.arn}/*"
      ]
    }]
  })
}

# Crawler keeps the raw zone's table definition in sync — the curated zone's
# table is instead registered explicitly by the Glue ETL job (see glue_jobs/),
# since that job writes Iceberg tables and controls the schema directly.
resource "aws_glue_crawler" "raw_zone" {
  name          = "${var.project_name}-raw-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.lakehouse.name
    configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableLevelConfiguration = 3
      TableGroupingPolicy     = "CombineCompatibleSchemas"
    }
  })

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/banking/raw/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_job" "curate_transactions" {
  name              = "${var.project_name}-curate-transactions"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/scripts/curate_transactions_job.py"
    python_version   = "3"
  }

  default_arguments = {
    "--conf" = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.bucket}/banking/iceberg_curated/ --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--datalake-formats"                = "iceberg"
    "--raw_path" = "s3://${aws_s3_bucket.lakehouse.bucket}/banking/raw/"
    "--warehouse_path" = "s3://${aws_s3_bucket.lakehouse.bucket}/banking/iceberg_curated/"
    "--catalog_database"                = aws_glue_catalog_database.lakehouse.name
    "--catalog_table" = "banking_curated_iceberg"
    "--job-language"                    = "python"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}

# ── Athena ───────────────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "lakehouse" {
  name = "${var.project_name}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"
    }
  }
}

# ── Redshift Serverless (query layer / BI marts) ────────────────────────────
# Gated behind enable_redshift (default false) — see variables.tf. Serverless
# bills per RPU-second while queries run, so the safest pattern for a demo
# is: set enable_redshift=true, apply, run your screenshot queries, then set
# it back to false and apply again to tear it down — rather than leaving it
# provisioned.

resource "aws_iam_role" "redshift_role" {
  count = var.enable_redshift ? 1 : 0
  name  = "${var.project_name}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3_access" {
  count = var.enable_redshift ? 1 : 0
  name  = "${var.project_name}-redshift-s3-access"
  role  = aws_iam_role.redshift_role[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket",
        "glue:GetCatalog",
        "glue:GetDatabases",
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_redshiftserverless_namespace" "lakehouse" {
  count                = var.enable_redshift ? 1 : 0
  namespace_name       = "${var.project_name}-ns"
  admin_username       = "admin"
  admin_user_password  = var.redshift_admin_password
  iam_roles            = [aws_iam_role.redshift_role[0].arn]
  default_iam_role_arn = aws_iam_role.redshift_role[0].arn
  db_name              = "bankingdb"
}

resource "aws_redshiftserverless_workgroup" "lakehouse" {
  count                = var.enable_redshift ? 1 : 0
  namespace_name       = aws_redshiftserverless_namespace.lakehouse[0].namespace_name
  workgroup_name       = "${var.project_name}-wg"
  base_capacity        = var.redshift_base_rpu
  publicly_accessible  = true
}
