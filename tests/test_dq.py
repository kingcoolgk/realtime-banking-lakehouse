import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession

from dq.dq_checks import (
    run_source_dq_checks,
    run_curated_dq_checks,
)

from producer.transaction_schema import TRANSACTION_SCHEMA


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TwoStageDQTest")
    .getOrCreate()
)

data = [
    (
        "TXN001", "CARD001", "CUST001", 500.0, "INR",
        "VISA", "Amazon", "ECOMMERCE", "Mumbai",
        "SUCCESS", False, "2026-09-08T10:00:00", "2026-09-08"
    ),
    (
        "TXN002", "CARD002", "CUST002", 1200.0, "INR",
        "MASTERCARD", "Swiggy", "RESTAURANT", "Delhi",
        "SUCCESS", False, "2026-09-08T10:01:00", "2026-09-08"
    ),
]

df = spark.createDataFrame(
    data,
    schema=TRANSACTION_SCHEMA
)

# -----------------------------
# Stage 1: Source DQ
# -----------------------------

source_df, source_report = run_source_dq_checks(
    df,
    9999
)

print("SOURCE DQ REPORT:")
print(source_report)

# Only test Stage 2 if Stage 1 passes.
if source_report["batch_passed"]:

    # Simulate transformation by adding a derived column.
    transformed_df = source_df.withColumn(
        "transaction_amount_inr",
        source_df["amount"]
    )

    # -----------------------------
    # Stage 2: Curated DQ
    # -----------------------------

    curated_df, curated_report = run_curated_dq_checks(
        transformed_df,
        9999
    )

    print("CURATED DQ REPORT:")
    print(curated_report)

    print("CURATED COLUMNS:")
    print(curated_df.columns)

spark.stop()
