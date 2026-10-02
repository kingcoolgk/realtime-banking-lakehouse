"""
consumer/stream_consumer.py
---------------------------
PySpark Structured Streaming consumer.

Flow:
    Kafka
      |
      v
    Source / Ingestion DQ
      |
      +---- FAIL --> Raw S3 + Quarantine + AIRFLOW_DQ_REPORT
      |
      v
    Raw S3
      |
      v
    Business Transformations
      |
      v
    Curated / Business DQ
      |
      +---- FAIL --> Quarantine + AIRFLOW_DQ_REPORT
      |
      v
    Curated S3 / Iceberg

Run:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      consumer/stream_consumer.py
"""

import json
import logging
import os
import sys

import yaml
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
)

sys.path.append(".")

from producer.transaction_schema import TRANSACTION_SCHEMA
from consumer.transformations import apply_transformations
from dq.dq_checks import (
    run_source_dq_checks,
    run_curated_dq_checks,
)
from storage.s3_writer import (
    write_to_raw_zone,
    write_to_curated_zone,
    write_dq_report,
    write_to_quarantine_zone,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s",
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# LOAD CONFIG
# ------------------------------------------------------------------------------

with open("config/config.yaml") as f:
    CONFIG = yaml.safe_load(f)

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    CONFIG["kafka"]["broker"],
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    CONFIG["kafka"]["topic"],
)

CHECKPOINT_PATH = CONFIG["spark"]["checkpoint_path"]

TRIGGER_INTERVAL = CONFIG["spark"]["trigger_interval"]


# ------------------------------------------------------------------------------
# SPARK SESSION
# ------------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("RealtimeBankingPipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )

    # AWS S3A authentication for AWS mode.
    if CONFIG["s3"]["mode"].lower() == "aws":
        builder = (
            builder
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider",
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint.region",
                CONFIG["s3"]["region"],
            )
        )

    return builder.getOrCreate()


# ------------------------------------------------------------------------------
# MICRO-BATCH PROCESSING
# ------------------------------------------------------------------------------

def process_batch(batch_df, batch_id: int):
    """
    Micro-batch processing flow:

        Kafka
          |
          v
        Source DQ #1
          |
          v
        Raw S3
          |
          v
        Transformations
          |
          v
        Curated DQ #2
          |
          v
        Curated S3 / Iceberg

    Raw data is always preserved for audit/replay.
    Curated data is written only after both DQ stages pass.
    """

    # ------------------------------------------------------------------
    # 0. EMPTY BATCH
    # ------------------------------------------------------------------

    if batch_df.isEmpty():
        logger.info(
            f"Batch {batch_id}: empty — skipping data processing."
        )

        # Airflow expects a DQ report from every streaming invocation.
        # An empty batch is not a DQ failure.
        airflow_dq_report = {
            "batch_id": batch_id,
            "batch_passed": True,
            "checks": [],
            "failures": [],
            "warnings": ["empty_batch"],
        }

        logger.info(
            "AIRFLOW_DQ_REPORT=" + json.dumps(
                airflow_dq_report,
                separators=(",", ":"),
            )
        )

        return

    record_count = batch_df.count()

    logger.info(
        f"Batch {batch_id}: received {record_count} records."
    )

    # ------------------------------------------------------------------
    # 1. SOURCE / INGESTION DQ
    # ------------------------------------------------------------------

    source_df, source_dq_report = run_source_dq_checks(
        batch_df,
        batch_id,
    )

    # Persist source DQ report.
    write_dq_report(
        source_dq_report,
        batch_id,
        batch_df.sparkSession,
    )

    # ------------------------------------------------------------------
    # 2. RAW S3
    # ------------------------------------------------------------------
    # Preserve the original Kafka-derived records regardless of DQ result.

    write_to_raw_zone(
        batch_df,
        batch_id,
    )

    # ------------------------------------------------------------------
    # SOURCE DQ FAILURE
    # ------------------------------------------------------------------

    if not source_dq_report["batch_passed"]:

        logger.warning(
            f"Batch {batch_id}: SOURCE DQ FAILED. "
            f"Failures: {source_dq_report['failures']}. "
            f"Raw data preserved; skipping transformation and curated write."
        )

        # Write failed source records to quarantine.
        write_to_quarantine_zone(
            batch_df,
            batch_id,
            reason="source_dq_failure",
        )

        airflow_dq_report = {
            "batch_id": batch_id,
            "batch_passed": False,
            "checks": source_dq_report.get("checks", []),
            "failures": source_dq_report.get("failures", []),
            "warnings": source_dq_report.get("warnings", []),
        }

        logger.info(
            "AIRFLOW_DQ_REPORT=" + json.dumps(
                airflow_dq_report,
                separators=(",", ":"),
            )
        )

        return

    # ------------------------------------------------------------------
    # 3. BUSINESS TRANSFORMATIONS
    # ------------------------------------------------------------------

    transformed_df = apply_transformations(
        source_df,
    )

    logger.info(
        f"Batch {batch_id}: business transformations completed."
    )

    # ------------------------------------------------------------------
    # 4. CURATED / BUSINESS DQ
    # ------------------------------------------------------------------

    curated_df, curated_dq_report = run_curated_dq_checks(
        transformed_df,
        batch_id,
    )

    # Persist curated-stage DQ report.
    # Use a different logical batch identifier so the two reports
    # cannot overwrite each other.

    curated_report_batch_id = f"{batch_id}_curated"

    write_dq_report(
        curated_dq_report,
        curated_report_batch_id,
        batch_df.sparkSession,
    )

    # ------------------------------------------------------------------
    # 5. CURATED S3 / LAKEHOUSE
    # ------------------------------------------------------------------

    if curated_dq_report["batch_passed"]:

        write_to_curated_zone(
            curated_df,
            batch_id,
        )

        logger.info(
            f"Batch {batch_id}: CURATED DQ PASSED. "
            f"{record_count} records → curated zone."
        )

    else:

        logger.warning(
            f"Batch {batch_id}: CURATED DQ FAILED. "
            f"Failures: {curated_dq_report['failures']}. "
            f"Raw data remains available; curated write skipped."
        )

        # Quarantine transformed data that failed curated DQ.
        write_to_quarantine_zone(
            transformed_df,
            batch_id,
            reason="curated_dq_failure",
        )

    # ------------------------------------------------------------------
    # 6. COMBINED AIRFLOW DQ REPORT
    # ------------------------------------------------------------------

    airflow_dq_report = {
        "batch_id": batch_id,

        "batch_passed": (
            source_dq_report["batch_passed"]
            and curated_dq_report["batch_passed"]
        ),

        "checks": (
            source_dq_report.get("checks", [])
            + curated_dq_report.get("checks", [])
        ),

        "failures": (
            source_dq_report.get("failures", [])
            + curated_dq_report.get("failures", [])
        ),

        "warnings": (
            source_dq_report.get("warnings", [])
            + curated_dq_report.get("warnings", [])
        ),
    }

    logger.info(
        "AIRFLOW_DQ_REPORT=" + json.dumps(
            airflow_dq_report,
            separators=(",", ":"),
        )
    )


# ------------------------------------------------------------------------------
# STREAMING
# ------------------------------------------------------------------------------

def run_streaming():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"Connecting to Kafka broker: {KAFKA_BROKER} | "
        f"Topic: {TOPIC}"
    )

    # ------------------------------------------------------------------
    # READ FROM KAFKA
    # ------------------------------------------------------------------

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BROKER,
        )
        .option(
            "subscribe",
            TOPIC,
        )
        .option(
            "startingOffsets",
            "latest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .option(
            "maxOffsetsPerTrigger",
            5000,
        )
        .load()
    )

    # ------------------------------------------------------------------
    # DESERIALIZE JSON PAYLOAD
    # ------------------------------------------------------------------

    parsed_stream = (
        raw_stream
        .select(
            from_json(
                col("value").cast("string"),
                TRANSACTION_SCHEMA,
            ).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "data.*",
            "kafka_timestamp",
        )
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp")),
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
        .withWatermark(
            "event_timestamp",
            "5 minutes",
        )
    )

    # ------------------------------------------------------------------
    # FOREACH BATCH
    # ------------------------------------------------------------------

    query = (
        parsed_stream.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )
        .trigger(
            processingTime=TRIGGER_INTERVAL,
        )
        .start()
    )

    logger.info(
        "Streaming query started. Awaiting termination..."
    )

    query.awaitTermination()


# ------------------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    run_streaming()