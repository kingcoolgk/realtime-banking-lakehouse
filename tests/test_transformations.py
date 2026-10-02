import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession

from consumer.transformations import apply_transformations
from producer.transaction_schema import TRANSACTION_SCHEMA


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("TransformationTest")
    .getOrCreate()
)

data = [
    (
        "TXN001", "CARD001", "CUST001", 60000.0, "INR",
        "VISA", "Amazon", "ECOMMERCE", "Mumbai",
        "SUCCESS", True, "2026-09-08T10:00:00", "2026-09-08"
    ),
    (
        "TXN002", "CARD002", "CUST002", 2500.0, "INR",
        "MASTERCARD", "Swiggy", "RESTAURANT", "Delhi",
        "SUCCESS", False, "2026-09-08T10:01:00", "2026-09-08"
    ),
    (
        "TXN003", "CARD003", "CUST003", 22000.0, "INR",
        "RUPAY", "MakeMyTrip", "TRAVEL", "Bengaluru",
        "FAILED", False, "2026-09-08T10:02:00", "2026-09-08"
    ),
]

df = spark.createDataFrame(
    data,
    schema=TRANSACTION_SCHEMA
)

result = apply_transformations(df)

result.select(
    "transaction_id",
    "amount",
    "is_international",
    "status",
    "merchant_category",
    "risk_score",
    "risk_label",
    "category_group",
    "amount_bucket",
    "is_high_value",
).show(truncate=False)

spark.stop()
