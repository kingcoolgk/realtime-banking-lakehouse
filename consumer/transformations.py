"""
consumer/transformations.py
-----------------------------
Business transformation and feature engineering logic.

Transformations:
    - risk_score
    - risk_label
    - category_group
    - amount_bucket
    - is_high_value
    - amount rounding

Implementation uses native Spark SQL expressions instead of Python UDFs.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    round as spark_round,
    create_map,
)


# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------

CATEGORY_MAP = {
    "GROCERY": "ESSENTIAL",
    "UTILITIES": "ESSENTIAL",
    "HEALTHCARE": "ESSENTIAL",
    "FUEL": "TRANSPORT",
    "TRAVEL": "TRANSPORT",
    "RESTAURANT": "LIFESTYLE",
    "ENTERTAINMENT": "LIFESTYLE",
    "ECOMMERCE": "DIGITAL",
}


# ---------------------------------------------------------------------------
# Main transformation
# ---------------------------------------------------------------------------

def apply_transformations(df: DataFrame) -> DataFrame:
    """
    Apply business transformations and feature engineering.

    Added columns:
        risk_score
        risk_label
        category_group
        amount_bucket
        is_high_value
    """

    # ------------------------------------------------------------------
    # 1. Risk score
    #
    # Amount contribution:
    #   > 50,000 INR  -> +0.40
    #   > 20,000 INR  -> +0.20
    #
    # Other signals:
    #   international -> +0.30
    #   failed        -> +0.20
    #   travel/ecommerce -> +0.10
    # ------------------------------------------------------------------

    amount_risk = (
        when(col("amount") > 50000, lit(0.40))
        .when(col("amount") > 20000, lit(0.20))
        .otherwise(lit(0.0))
    )

    international_risk = when(
        col("is_international") == True,
        lit(0.30)
    ).otherwise(lit(0.0))

    status_risk = when(
        col("status") == "FAILED",
        lit(0.20)
    ).otherwise(lit(0.0))

    category_risk = when(
        col("merchant_category").isin("TRAVEL", "ECOMMERCE"),
        lit(0.10)
    ).otherwise(lit(0.0))

    risk_score = (
        amount_risk
        + international_risk
        + status_risk
        + category_risk
    )

    df = df.withColumn(
        "risk_score",
        spark_round(
            when(risk_score > 1.0, lit(1.0))
            .otherwise(risk_score),
            2
        )
    )

    # ------------------------------------------------------------------
    # 2. Risk label
    # ------------------------------------------------------------------

    df = df.withColumn(
        "risk_label",
        when(col("risk_score") >= 0.7, lit("HIGH"))
        .when(col("risk_score") >= 0.4, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )

    # ------------------------------------------------------------------
    # 3. Merchant category normalization
    # ------------------------------------------------------------------

    category_mapping = create_map(
        *[
            item
            for pair in CATEGORY_MAP.items()
            for item in (lit(pair[0]), lit(pair[1]))
        ]
    )

    df = df.withColumn(
        "category_group",
        when(
            col("merchant_category").isNull(),
            lit("OTHER")
        )
        .otherwise(
            category_mapping.getItem(col("merchant_category"))
        )
    )

    df = df.fillna(
        {"category_group": "OTHER"}
    )

    # ------------------------------------------------------------------
    # 4. Transaction amount bucket
    # ------------------------------------------------------------------

    df = df.withColumn(
        "amount_bucket",
        when(col("amount") < 500, lit("MICRO"))
        .when(col("amount") < 5000, lit("SMALL"))
        .when(col("amount") < 20000, lit("MEDIUM"))
        .when(col("amount") < 50000, lit("LARGE"))
        .otherwise(lit("HIGH_VALUE"))
    )

    # ------------------------------------------------------------------
    # 5. High-value transaction flag
    # ------------------------------------------------------------------

    df = df.withColumn(
        "is_high_value",
        col("amount") > 50000
    )

    # ------------------------------------------------------------------
    # 6. Monetary precision
    # ------------------------------------------------------------------

    df = df.withColumn(
        "amount",
        spark_round(col("amount"), 2)
    )

    return df