"""
storage/s3_writer.py
----------------------
Writes processed DataFrames to local filesystem (demo mode) or real AWS S3
via s3a://, switched by config.yaml's s3.mode.

Real S3 writes need the Hadoop AWS connector on the Spark classpath:
    spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 consumer/stream_consumer.py

AWS credentials are picked up from the default provider chain
(~/.aws/credentials, env vars, or an instance/role profile) — never hardcode
keys here.
"""

import logging
import os
import yaml

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

with open("config/config.yaml") as f:
    CONFIG = yaml.safe_load(f)

S3_MODE = CONFIG["s3"].get("mode", "local")

if S3_MODE == "aws":
    _bucket = CONFIG["s3"]["bucket"]
    RAW_PREFIX = f's3a://{_bucket}/{CONFIG["s3"]["raw_prefix"]}'
    CURATED_PREFIX = f's3a://{_bucket}/{CONFIG["s3"]["curated_prefix"]}'
    DQ_PREFIX = f's3a://{_bucket}/{CONFIG["s3"]["dq_prefix"]}'
else:
    RAW_PREFIX = CONFIG["s3"]["raw_prefix"]
    CURATED_PREFIX = CONFIG["s3"]["curated_prefix"]
    DQ_PREFIX = CONFIG["s3"]["dq_prefix"]

    # Local demo mode only — real S3 prefixes don't exist on the local filesystem.
    os.makedirs(RAW_PREFIX, exist_ok=True)
    os.makedirs(CURATED_PREFIX, exist_ok=True)
    os.makedirs(DQ_PREFIX, exist_ok=True)

logger.info(f"s3_writer initialised in '{S3_MODE}' mode. raw={RAW_PREFIX}")


def write_to_raw_zone(df: DataFrame, batch_id: int):
    """
    Write all incoming records to the raw zone as an audit trail.
    Partitioned by event_date and card_type for efficient time-range scans.
    """
    raw_path = RAW_PREFIX

    try:
        (
            df.write
            .mode("append")
            .partitionBy("event_date", "card_type")
            .parquet(raw_path)
        )
        logger.info(f"Batch {batch_id} | Raw zone write SUCCESS → {raw_path}")
    except Exception as e:
        logger.error(f"Batch {batch_id} | Raw zone write FAILED: {e}")
        raise


def write_to_curated_zone(df: DataFrame, batch_id: int):
    """
    Write DQ-passed, deduplicated records to the curated zone.
    Deduplication on transaction_id ensures idempotent micro-batch writes.
    Partitioned by event_date + category_group for BI query optimisation.
    """
    curated_path = CURATED_PREFIX

    # Deduplicate within the batch on transaction_id
    deduped_df = df.dropDuplicates(["transaction_id"])

    try:
        (
            deduped_df.write
            .mode("append")
            .partitionBy("event_date", "category_group")
            .parquet(curated_path)
        )
        logger.info(f"Batch {batch_id} | Curated zone write SUCCESS → {curated_path}")
    except Exception as e:
        logger.error(f"Batch {batch_id} | Curated zone write FAILED: {e}")
        raise


def write_dq_report(report: dict, batch_id: int, spark):
    """
    Persist the DQ report dict as a single-row JSON Parquet for audit/monitoring.
    """
    import json

    dq_path = DQ_PREFIX
    rdd = spark.sparkContext.parallelize([json.dumps(report)])
    dq_df = spark.read.json(rdd)

    try:
        dq_df.write.mode("append").parquet(dq_path)
        logger.info(f"Batch {batch_id} | DQ report written → {dq_path}")
    except Exception as e:
        logger.warning(
            f"Batch {batch_id} | DQ report write failed (non-critical): {e}"
        )