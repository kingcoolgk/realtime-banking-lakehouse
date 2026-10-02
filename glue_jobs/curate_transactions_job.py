"""
glue_jobs/curate_transactions_job.py
--------------------------------------
AWS Glue ETL job (Glue 4.0, Spark + Iceberg).

Reads Parquet from the S3 raw zone, applies curated-layer cleansing
and deterministic business transformations, then writes the result
to an Apache Iceberg table registered in the AWS Glue Data Catalog.

Flow:
    S3 Raw Parquet
        -> critical-field filtering
        -> transaction_id deduplication
        -> event_date normalization
        -> category_group derivation
        -> stable curated schema
        -> Iceberg table / MERGE
        -> Glue Data Catalog
        -> Athena / Redshift Spectrum
"""

import sys
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    to_date,
    upper,
    when,
    least, lit, round
)


# ---------------------------------------------------------------------------
# Job arguments
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "raw_path",
        "warehouse_path",
        "catalog_database",
        "catalog_table",
    ],
)


# ---------------------------------------------------------------------------
# Spark / Glue initialization
# ---------------------------------------------------------------------------

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------------------------
# Iceberg catalog
#
# spark.sql.extensions and the other Iceberg Spark settings are supplied
# through Terraform at Glue job startup. They should NOT be modified here
# after the Spark session has already been created.
# ---------------------------------------------------------------------------

TABLE_FQN = (
    f'glue_catalog.{args["catalog_database"]}.{args["catalog_table"]}'
)


# ---------------------------------------------------------------------------
# Schema contract
# ---------------------------------------------------------------------------

CRITICAL_FIELDS = [
    "transaction_id",
    "card_id",
    "amount",
    "event_timestamp",
    "event_date",
]

CURATED_COLUMNS = [
    "transaction_id",
    "card_id",
    "customer_id",
    "amount",
    "currency",
    "card_type",
    "merchant_name",
    "merchant_category",
    "city",
    "status",
    "is_international",
    "event_timestamp",
    "kafka_timestamp",
    "ingestion_timestamp",
    "risk_score",
    "risk_label",
    "amount_bucket",
    "is_high_value",
    "event_date",
    "category_group",
]


# ---------------------------------------------------------------------------
# Read raw zone
# ---------------------------------------------------------------------------

def read_raw_zone():
    """
    Read the production raw Parquet zone.

    The raw zone is partitioned by event_date and card_type, so Spark
    discovers those partition columns while reading from the root path.
    """

    return (
        spark.read
        .option("mergeSchema", "true")
        .parquet(args["raw_path"])
    )


# ---------------------------------------------------------------------------
# Derive business fields
# ---------------------------------------------------------------------------

def add_category_group(df):
    """
    Derive category_group from merchant_category.

    Mapping mirrors the transformation logic used by the streaming
    curated pipeline:

        GROCERY / UTILITIES / HEALTHCARE -> ESSENTIAL
        FUEL / TRAVEL                  -> TRANSPORT
        RESTAURANT / ENTERTAINMENT     -> LIFESTYLE
        ECOMMERCE                      -> DIGITAL
        everything else               -> OTHER
    """

    merchant_category = upper(col("merchant_category"))

    return df.withColumn(
        "category_group",
        when(
            merchant_category.isin(
                "GROCERY",
                "UTILITIES",
                "HEALTHCARE",
            ),
            "ESSENTIAL",
        )
        .when(
            merchant_category.isin(
                "FUEL",
                "TRAVEL",
            ),
            "TRANSPORT",
        )
        .when(
            merchant_category.isin(
                "RESTAURANT",
                "ENTERTAINMENT",
            ),
            "LIFESTYLE",
        )
        .when(
            merchant_category == "ECOMMERCE",
            "DIGITAL",
        )
        .otherwise("OTHER"),
    )


# ---------------------------------------------------------------------------
# Curated transformation
# ---------------------------------------------------------------------------

def apply_curation(df):
    """
    Apply the same business transformations used by the streaming pipeline,
    then enforce critical-field filtering and transaction-level deduplication.
    """

    # Critical-field filtering
    for field in CRITICAL_FIELDS:
        df = df.filter(col(field).isNotNull())

    # ------------------------------------------------------------
    # Category grouping
    # ------------------------------------------------------------
    df = df.withColumn(
        "category_group",
        when(
            col("merchant_category").isin(
                "GROCERY", "UTILITIES", "HEALTHCARE"
            ),
            "ESSENTIAL",
        )
        .when(
            col("merchant_category").isin(
                "FUEL", "TRAVEL"
            ),
            "TRANSPORT",
        )
        .when(
            col("merchant_category").isin(
                "RESTAURANT", "ENTERTAINMENT"
            ),
            "LIFESTYLE",
        )
        .when(
            col("merchant_category") == "ECOMMERCE",
            "DIGITAL",
        )
        .otherwise("OTHER"),
    )

    # ------------------------------------------------------------
    # Risk score
    # ------------------------------------------------------------
    amount_score = (
        when(col("amount") > 50000, lit(0.40))
        .when(col("amount") > 20000, lit(0.20))
        .otherwise(lit(0.0))
    )

    international_score = when(
        col("is_international") == True,
        lit(0.30),
    ).otherwise(lit(0.0))

    failed_score = when(
        col("status") == "FAILED",
        lit(0.20),
    ).otherwise(lit(0.0))

    category_score = when(
        col("merchant_category").isin("TRAVEL", "ECOMMERCE"),
        lit(0.10),
    ).otherwise(lit(0.0))

    df = df.withColumn(
        "risk_score",
        least(
            lit(1.0),
            amount_score
            + international_score
            + failed_score
            + category_score,
        ),
    )

    # ------------------------------------------------------------
    # Risk label
    # ------------------------------------------------------------
    df = df.withColumn(
        "risk_label",
        when(col("risk_score") >= 0.70, "HIGH")
        .when(col("risk_score") >= 0.40, "MEDIUM")
        .otherwise("LOW"),
    )

    # ------------------------------------------------------------
    # Amount bucket
    # ------------------------------------------------------------
    df = df.withColumn(
        "amount_bucket",
        when(col("amount") < 500, "MICRO")
        .when(col("amount") < 5000, "SMALL")
        .when(col("amount") < 20000, "MEDIUM")
        .when(col("amount") < 50000, "LARGE")
        .otherwise("HIGH_VALUE"),
    )

    # ------------------------------------------------------------
    # High-value flag
    # ------------------------------------------------------------
    df = df.withColumn(
        "is_high_value",
        col("amount") >= 50000,
    )

    # Round amount consistently
    df = df.withColumn(
        "amount",
        round(col("amount"), 2),
    )

    # Final deduplication
    return df.dropDuplicates(["transaction_id"])


# ---------------------------------------------------------------------------
# Glue Catalog table existence
# ---------------------------------------------------------------------------

def iceberg_table_exists():
    """
    Check whether the target Iceberg table already exists in Glue Catalog.

    This avoids Spark catalog table-existence parsing issues and uses the
    same Glue Data Catalog that owns the Iceberg table.
    """

    glue = boto3.client("glue")

    try:
        glue.get_table(
            DatabaseName=args["catalog_database"],
            Name=args["catalog_table"],
        )
        return True

    except glue.exceptions.EntityNotFoundException:
        return False


# ---------------------------------------------------------------------------
# Iceberg writer
# ---------------------------------------------------------------------------

def write_curated_iceberg(df):
    """
    Create the Iceberg table on first execution.

    On later executions, MERGE only previously unseen transaction IDs.
    """

    table_exists = iceberg_table_exists()

    if not table_exists:

        print(
            f"Creating new Iceberg table: {TABLE_FQN}"
        )

        try:
            df.createOrReplaceTempView("curated_batch")

            spark.sql(
                f"""
                CREATE TABLE {TABLE_FQN}
                USING iceberg
                PARTITIONED BY (
                    event_date,
                    category_group
                )
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.spark.fanout.enabled' = 'true'
                )
                AS
                SELECT
                    transaction_id,
                    card_id,
                    customer_id,
                    amount,
                    currency,
                    card_type,
                    merchant_name,
                    merchant_category,
                    city,
                    status,
                    is_international,
                    event_timestamp,
                    kafka_timestamp,
                    ingestion_timestamp,
                    risk_score,
                    risk_label,
                    amount_bucket,
                    is_high_value,
                    event_date,
                    category_group
                FROM curated_batch
                """
            )

        except Exception as e:
            print("ICEBERG_CREATE_SQL_ERROR:", repr(e))

            java_exception = getattr(e, "java_exception", None)

            if java_exception is not None:
                cause = java_exception

                for i in range(10):
                    try:
                        print(
                            f"ICEBERG_SQL_CAUSE_{i}:",
                            cause.toString()
                        )
                    except Exception:
                        break

                    next_cause = cause.getCause()

                    if next_cause is None:
                        break

                    cause = next_cause

                try:
                    root_message = cause.toString()
                except Exception:
                    root_message = str(e)

            else:
                root_message = str(e)

            raise RuntimeError(
                f"ICEBERG_SQL_ROOT_CAUSE: {root_message}"
            ) from e


        print(
            f"Created Iceberg table: {TABLE_FQN}"
        )

        return

    print(
        f"Iceberg table already exists: {TABLE_FQN}"
    )

    df.createOrReplaceTempView("curated_batch")

    spark.sql(
        f"""
        MERGE INTO {TABLE_FQN} AS target
        USING curated_batch AS source
        ON target.transaction_id = source.transaction_id

        WHEN NOT MATCHED THEN INSERT (
            transaction_id,
            card_id,
            customer_id,
            amount,
            currency,
            card_type,
            merchant_name,
            merchant_category,
            city,
            status,
            is_international,
            event_timestamp,
            kafka_timestamp,
            ingestion_timestamp,
            risk_score,
            risk_label,
            amount_bucket,
            is_high_value,
            event_date,
            category_group
        )
        VALUES (
            source.transaction_id,
            source.card_id,
            source.customer_id,
            source.amount,
            source.currency,
            source.card_type,
            source.merchant_name,
            source.merchant_category,
            source.city,
            source.status,
            source.is_international,
            source.event_timestamp,
            source.kafka_timestamp,
            source.ingestion_timestamp,
            source.risk_score,
            source.risk_label,
            source.amount_bucket,
            source.is_high_value,
            source.event_date,
            source.category_group
        )
        """
    )

    print(
        f"Merged curated batch into: {TABLE_FQN}"
    )

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():

    print(
        f"Reading raw data from: {args['raw_path']}"
    )

    raw_df = read_raw_zone()

    print("Raw schema:")
    raw_df.printSchema()

    curated_df = apply_curation(raw_df)

    row_count = curated_df.count()

    print(
        f"Curating {row_count} deduplicated rows into {TABLE_FQN}"
    )

    print("Curated schema:")
    curated_df.printSchema()

    write_curated_iceberg(curated_df)

    job.commit()

    print(
        f"Glue job completed successfully for {TABLE_FQN}"
    )


if __name__ == "__main__":
    main()