"""
dq/dq_checks.py
----------------
Two-stage Data Quality engine.

Stage 1: Source / Ingestion DQ
    Validates the contract of data arriving from Kafka.

Stage 2: Curated / Business DQ
    Validates data after business transformations and enrichment.

Hard failures block promotion to the curated layer.
Statistical anomalies are soft warnings.
"""

import logging
from typing import Tuple, Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    isnan,
    isnull,
    mean,
    stddev,
    to_date,
    to_timestamp,
    count,
    sum,
    when,
)

from producer.transaction_schema import (
    CRITICAL_FIELDS,
    EXPECTED_FIELDS,
)
from dq.dq_alert import send_dq_alert

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Thresholds / business rules
# ---------------------------------------------------------------------------

NULL_RATE_THRESHOLD = 0.02
ANOMALY_ZSCORE_CUTOFF = 4.0
MIN_RECORDS_FOR_STATS = 30

VALID_CARD_TYPES = {
    "VISA",
    "MASTERCARD",
    "RUPAY",
    "AMEX",
}

VALID_STATUSES = {
    "SUCCESS",
    "FAILED",
    "PENDING",
}

EXPECTED_CURRENCY = "INR"
# Pipeline-generated metadata fields are not part of the source payload
# contract and should not trigger source schema drift.
SOURCE_METADATA_FIELDS = {
    "kafka_timestamp",
    "ingestion_timestamp",
}

# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _empty_anomaly_column(df: DataFrame) -> DataFrame:
    """
    Add a deterministic False anomaly flag when statistics cannot be run.
    """
    return df.withColumn(
        "is_amount_anomaly",
        col("transaction_id").isNull() & col("transaction_id").isNotNull()
    )


# ---------------------------------------------------------------------------
# SOURCE DQ #1
# ---------------------------------------------------------------------------

def check_schema_drift(
    df: DataFrame,
    batch_id: int
) -> Dict[str, Any]:
    """
    Validate source schema contract.

    Missing or unexpected fields cause a hard failure.
    """

    actual_fields = set(df.columns)

    source_fields = actual_fields - SOURCE_METADATA_FIELDS

    missing_fields = EXPECTED_FIELDS - source_fields
    extra_fields = source_fields - EXPECTED_FIELDS

    passed = (
        len(missing_fields) == 0
        and len(extra_fields) == 0
    )

    result = {
        "check": "schema_drift",
        "passed": passed,
        "missing_fields": sorted(list(missing_fields)),
        "extra_fields": sorted(list(extra_fields)),
    }

    if passed:
        logger.info(
            f"Batch {batch_id} | Source schema check PASSED."
        )
    else:
        logger.error(
            f"Batch {batch_id} | SOURCE SCHEMA DRIFT | "
            f"Missing={missing_fields}, Extra={extra_fields}"
        )

    return result


def check_null_rates(
    df: DataFrame,
    batch_id: int
) -> Dict[str, Any]:
    """
    Validate null rates for critical source fields.

    Uses a single aggregation action for all critical fields to avoid
    repeated Spark scans of the same micro-batch.
    """

    failures = {}
    metrics = {}

    aggregation_exprs = []

    valid_fields = [
        field for field in CRITICAL_FIELDS
        if field in df.columns
    ]

    for field in valid_fields:
        data_type = df.schema[field].dataType.typeName()

        if data_type in ("double", "float"):
            null_condition = (
                isnull(col(field)) | isnan(col(field))
            )
        else:
            null_condition = isnull(col(field))

        aggregation_exprs.append(
            # Total records is calculated once below.
            sum(when(null_condition, 1).otherwise(0)).alias(
                f"{field}__null_count"
            )
        )

    if not aggregation_exprs:
        return {
            "check": "null_rates",
            "passed": True,
            "failures": {},
            "metrics": {},
            "total_records": 0,
        }


    result = df.agg(
        count("*").alias("__total_records"),
        *aggregation_exprs
    ).collect()[0]

    total = result["__total_records"]

    if total == 0:
        return {
            "check": "null_rates",
            "passed": True,
            "failures": {},
            "metrics": {},
            "total_records": 0,
        }

    for field in valid_fields:
        null_count = result[f"{field}__null_count"] or 0
        null_rate = null_count / total

        metrics[field] = {
            "null_count": null_count,
            "null_rate": round(null_rate, 4),
        }

        if null_rate > NULL_RATE_THRESHOLD:
            failures[field] = round(null_rate, 4)

    passed = len(failures) == 0

    if passed:
        logger.info(
            f"Batch {batch_id} | Source null-rate check PASSED."
        )
    else:
        logger.error(
            f"Batch {batch_id} | SOURCE NULL BREACH | {failures}"
        )

    return {
        "check": "null_rates",
        "passed": passed,
        "failures": failures,
        "metrics": metrics,
        "total_records": total,
    }


def check_domain_rules(
    df: DataFrame,
    batch_id: int
) -> Dict[str, Any]:
    """
    Validate controlled business-domain values from the source.
    """

    failures = {}

    invalid_card_type = df.filter(
        col("card_type").isNotNull()
        & (~col("card_type").isin(*VALID_CARD_TYPES))
    ).count()

    invalid_status = df.filter(
        col("status").isNotNull()
        & (~col("status").isin(*VALID_STATUSES))
    ).count()

    invalid_currency = df.filter(
        col("currency").isNotNull()
        & (col("currency") != EXPECTED_CURRENCY)
    ).count()

    if invalid_card_type > 0:
        failures["invalid_card_type"] = invalid_card_type

    if invalid_status > 0:
        failures["invalid_status"] = invalid_status

    if invalid_currency > 0:
        failures["invalid_currency"] = invalid_currency

    passed = len(failures) == 0

    if passed:
        logger.info(
            f"Batch {batch_id} | Source domain check PASSED."
        )
    else:
        logger.error(
            f"Batch {batch_id} | SOURCE DOMAIN BREACH | {failures}"
        )

    return {
        "check": "domain_rules",
        "passed": passed,
        "failures": failures,
    }


def check_amount_validity(
    df: DataFrame,
    batch_id: int
) -> Dict[str, Any]:
    """
    Validate source transaction amounts.
    """

    invalid_count = df.filter(
        col("amount").isNull()
        | isnan(col("amount"))
        | (col("amount") <= 0)
    ).count()

    passed = invalid_count == 0

    if passed:
        logger.info(
            f"Batch {batch_id} | Source amount validation PASSED."
        )
    else:
        logger.error(
            f"Batch {batch_id} | INVALID SOURCE AMOUNT | "
            f"{invalid_count} records"
        )

    return {
        "check": "amount_validity",
        "passed": passed,
        "invalid_count": invalid_count,
    }


def check_duplicate_transactions(
    df: DataFrame,
    batch_id: int
) -> Dict[str, Any]:
    """
    Detect duplicate transaction IDs within the micro-batch.
    """

    duplicate_count = (
        df.groupBy("transaction_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    passed = duplicate_count == 0

    if passed:
        logger.info(
            f"Batch {batch_id} | Source duplicate check PASSED."
        )
    else:
        logger.error(
            f"Batch {batch_id} | DUPLICATE SOURCE TRANSACTIONS | "
            f"{duplicate_count} duplicate IDs"
        )

    return {
        "check": "duplicate_transactions",
        "passed": passed,
        "duplicate_count": duplicate_count,
    }


def check_timestamp_validity(
    df: DataFrame,
    batch_id: int
) -> Dict[str, Any]:
    """
    Validate source event_timestamp and event_date.
    """

    invalid_timestamp = df.filter(
        to_timestamp(col("event_timestamp")).isNull()
    ).count()

    invalid_event_date = df.filter(
        to_date(col("event_date")).isNull()
    ).count()

    failures = {}

    if invalid_timestamp > 0:
        failures["invalid_timestamp"] = invalid_timestamp

    if invalid_event_date > 0:
        failures["invalid_event_date"] = invalid_event_date

    passed = len(failures) == 0

    if passed:
        logger.info(
            f"Batch {batch_id} | Source timestamp validation PASSED."
        )
    else:
        logger.error(
            f"Batch {batch_id} | SOURCE TIMESTAMP BREACH | {failures}"
        )

    return {
        "check": "timestamp_validity",
        "passed": passed,
        "failures": failures,
    }


def run_source_dq_checks(
    df: DataFrame,
    batch_id: int
) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Stage 1 DQ.

    Validates the Kafka/source contract before transformation.
    """

    report = {
        "stage": "source",
        "batch_id": batch_id,
        "checks": [],
        "failures": [],
        "warnings": [],
        "batch_passed": True,
    }

    checks = [
        ("schema_drift", check_schema_drift),
        ("null_rates", check_null_rates),
        ("domain_rules", check_domain_rules),
        ("amount_validity", check_amount_validity),
        ("duplicate_transactions", check_duplicate_transactions),
        ("timestamp_validity", check_timestamp_validity),
    ]

    for check_name, check_fn in checks:

        result = check_fn(df, batch_id)

        report["checks"].append(result)

        if not result["passed"]:
            report["batch_passed"] = False
            report["failures"].append(check_name)

            send_dq_alert(
                batch_id,
                check_name.upper(),
                result,
            )

    logger.info(
        f"Batch {batch_id} | SOURCE DQ COMPLETE | "
        f"passed={report['batch_passed']} | "
        f"failures={report['failures']}"
    )

    return df, report


# ---------------------------------------------------------------------------
# CURATED DQ #2
# ---------------------------------------------------------------------------

def check_curated_schema(
    df: DataFrame,
    batch_id: int
) -> Dict[str, Any]:
    """
    Basic sanity check for the post-transformation DataFrame.

    Curated transformations are expected to retain the source contract
    and may add derived/enrichment columns.
    """

    required_fields = set(EXPECTED_FIELDS)

    missing_fields = required_fields - set(df.columns)

    passed = len(missing_fields) == 0

    result = {
        "check": "curated_schema",
        "passed": passed,
        "missing_fields": sorted(list(missing_fields)),
    }

    if passed:
        logger.info(
            f"Batch {batch_id} | Curated schema check PASSED."
        )
    else:
        logger.error(
            f"Batch {batch_id} | CURATED SCHEMA BREACH | "
            f"Missing={missing_fields}"
        )

    return result


def check_curated_business_rules(
    df: DataFrame,
    batch_id: int
) -> Dict[str, Any]:
    """
    Validate the transformed dataset before lakehouse promotion.

    This catches corruption or unexpected behavior introduced by
    transformations/enrichment.
    """

    failures = {}

    # Transaction ID must remain unique after transformation.
    duplicate_count = (
        df.groupBy("transaction_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        failures["duplicate_transaction_ids"] = duplicate_count

    # Amount must remain valid after transformation.
    invalid_amounts = df.filter(
        col("amount").isNull()
        | isnan(col("amount"))
        | (col("amount") <= 0)
    ).count()

    if invalid_amounts > 0:
        failures["invalid_amounts"] = invalid_amounts

    # Source transaction identifiers must not disappear.
    missing_transaction_ids = df.filter(
        col("transaction_id").isNull()
    ).count()

    if missing_transaction_ids > 0:
        failures["missing_transaction_ids"] = missing_transaction_ids

    passed = len(failures) == 0

    if passed:
        logger.info(
            f"Batch {batch_id} | Curated business checks PASSED."
        )
    else:
        logger.error(
            f"Batch {batch_id} | CURATED BUSINESS BREACH | "
            f"{failures}"
        )

    return {
        "check": "curated_business_rules",
        "passed": passed,
        "failures": failures,
    }


def check_anomalies(
    df: DataFrame,
    batch_id: int
) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Statistical amount anomaly detection.

    Soft check: anomalies generate warnings but do not reject
    an otherwise valid curated batch.
    """

    total = df.count()

    if total < MIN_RECORDS_FOR_STATS:

        df = _empty_anomaly_column(df)

        return df, {
            "check": "anomaly_detection",
            "passed": True,
            "flagged_count": 0,
            "skipped": True,
            "reason": "insufficient_records",
        }

    stats = (
        df.select(
            mean(col("amount")).alias("mean_amt"),
            stddev(col("amount")).alias("std_amt"),
        )
        .collect()[0]
    )

    mean_val = stats["mean_amt"]
    std_val = stats["std_amt"]

    if mean_val is None or std_val is None or std_val == 0:

        df = _empty_anomaly_column(df)

        return df, {
            "check": "anomaly_detection",
            "passed": True,
            "flagged_count": 0,
            "skipped": True,
            "reason": "insufficient_amount_variance",
        }

    # Spark abs() expression without introducing another import.
    z_score = (col("amount") - mean_val) / std_val

    df = df.withColumn(
        "is_amount_anomaly",
        (z_score > ANOMALY_ZSCORE_CUTOFF)
        | (z_score < -ANOMALY_ZSCORE_CUTOFF)
    )

    flagged_count = df.filter(
        col("is_amount_anomaly") == True
    ).count()

    logger.info(
        f"Batch {batch_id} | Curated anomaly detection | "
        f"flagged={flagged_count}"
    )

    return df, {
        "check": "anomaly_detection",
        "passed": True,
        "flagged_count": flagged_count,
        "mean": round(mean_val, 2),
        "std": round(std_val, 2),
        "skipped": False,
    }


def run_curated_dq_checks(
    df: DataFrame,
    batch_id: int
) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Stage 2 DQ.

    Validates transformed data before curated/lakehouse promotion.
    """

    report = {
        "stage": "curated",
        "batch_id": batch_id,
        "checks": [],
        "failures": [],
        "warnings": [],
        "batch_passed": True,
    }

    # 1. Curated schema
    schema_result = check_curated_schema(df, batch_id)

    report["checks"].append(schema_result)

    if not schema_result["passed"]:
        report["batch_passed"] = False
        report["failures"].append("curated_schema")

        send_dq_alert(
            batch_id,
            "CURATED_SCHEMA_BREACH",
            schema_result,
        )

        return df, report

    # 2. Curated business consistency
    business_result = check_curated_business_rules(
        df,
        batch_id
    )

    report["checks"].append(business_result)

    if not business_result["passed"]:
        report["batch_passed"] = False
        report["failures"].append("curated_business_rules")

        send_dq_alert(
            batch_id,
            "CURATED_BUSINESS_BREACH",
            business_result,
        )

    # 3. Statistical anomaly detection
    df, anomaly_result = check_anomalies(
        df,
        batch_id
    )

    report["checks"].append(anomaly_result)

    if anomaly_result.get("flagged_count", 0) > 0:

        report["warnings"].append("anomaly_detection")

        send_dq_alert(
            batch_id,
            "ANOMALY_FLAG",
            anomaly_result,
            severity="WARNING",
        )

    logger.info(
        f"Batch {batch_id} | CURATED DQ COMPLETE | "
        f"passed={report['batch_passed']} | "
        f"failures={report['failures']} | "
        f"warnings={report['warnings']}"
    )

    return df, report