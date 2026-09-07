"""
dq/dq_checks.py
-----------------
Data Quality rule engine for the banking transaction pipeline.

Three DQ layers:
  1. Schema Drift Detection  — field-level presence check
  2. Null Threshold Checks   — critical field null rate < 2%
  3. Statistical Anomaly     — Z-score based amount outlier flagging
"""

import logging
from typing import Tuple, Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, mean, stddev

from producer.transaction_schema import CRITICAL_FIELDS, EXPECTED_FIELDS
from dq.dq_alert import send_dq_alert

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
NULL_RATE_THRESHOLD    = 0.02   # 2%  — max allowed null rate for critical fields
ANOMALY_ZSCORE_CUTOFF  = 4.0   # records beyond 4 std deviations flagged
MIN_RECORDS_FOR_STATS  = 30    # skip anomaly check if batch too small


def check_schema_drift(df: DataFrame, batch_id: int) -> Dict[str, Any]:
    """
    Compare incoming DataFrame columns against the registered schema.
    Returns a dict with drift details.
    """
    actual_fields   = set(df.columns)
    missing_fields  = EXPECTED_FIELDS - actual_fields
    extra_fields    = actual_fields - EXPECTED_FIELDS

    passed = len(missing_fields) == 0 and len(extra_fields) == 0
    result  = {
        "check":           "schema_drift",
        "passed":          passed,
        "missing_fields":  list(missing_fields),
        "extra_fields":    list(extra_fields),
    }

    if not passed:
        logger.error(f"Batch {batch_id} | SCHEMA DRIFT detected. Missing: {missing_fields}")
    else:
        logger.info(f"Batch {batch_id} | Schema drift check PASSED.")

    return result


def check_null_rates(df: DataFrame, batch_id: int) -> Dict[str, Any]:
    """
    Check null rate for each critical field.
    Fails if any field exceeds NULL_RATE_THRESHOLD.
    """
    total = df.count()
    if total == 0:
        return {"check": "null_rates", "passed": True, "details": {}}

    failures = {}
    for field in CRITICAL_FIELDS:
        if field not in df.columns:
            continue
        null_count = df.filter(isnull(col(field)) | isnan(col(field))).count() \
                     if df.schema[field].dataType.typeName() in ("double", "float") \
                     else df.filter(isnull(col(field))).count()
        rate = null_count / total
        if rate > NULL_RATE_THRESHOLD:
            failures[field] = round(rate, 4)
            logger.warning(f"Batch {batch_id} | NULL breach: {field} = {rate:.2%}")

    passed = len(failures) == 0
    if passed:
        logger.info(f"Batch {batch_id} | Null rate check PASSED.")
    return {"check": "null_rates", "passed": passed, "failures": failures}


def check_anomalies(df: DataFrame, batch_id: int) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Flag statistical outliers in the 'amount' column using Z-score.
    Returns annotated DataFrame and check result dict.
    """
    total = df.count()

    if total < MIN_RECORDS_FOR_STATS:
        logger.info(f"Batch {batch_id} | Too few records ({total}) for anomaly check — skipping.")
        df = df.withColumn("is_amount_anomaly", col("amount").isNull() & col("amount").isNotNull())  # all False
        return df, {"check": "anomaly_detection", "passed": True, "flagged_count": 0, "skipped": True}

    stats = df.select(
        mean(col("amount")).alias("mean_amt"),
        stddev(col("amount")).alias("std_amt")
    ).collect()[0]

    mean_val = stats["mean_amt"] or 0
    std_val  = stats["std_amt"]  or 1

    df = df.withColumn(
        "is_amount_anomaly",
        when(
            ((col("amount") - mean_val) / std_val) > ANOMALY_ZSCORE_CUTOFF, True
        ).otherwise(False)
    )

    flagged = df.filter(col("is_amount_anomaly") == True).count()

    logger.info(f"Batch {batch_id} | Anomaly check: {flagged} records flagged (Z > {ANOMALY_ZSCORE_CUTOFF}).")
    return df, {
        "check":         "anomaly_detection",
        "passed":        True,          # anomalies flagged, not blocked
        "flagged_count": flagged,
        "mean":          round(mean_val, 2),
        "std":           round(std_val, 2),
    }


def run_dq_checks(df: DataFrame, batch_id: int) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Master DQ runner. Executes all checks sequentially.

    Returns:
        (df_with_flags, dq_report)
        dq_report["batch_passed"] = False if any hard-fail check fails
    """
    report = {"batch_id": batch_id, "checks": [], "failures": [], "batch_passed": True}

    # 1. Schema drift — HARD FAIL
    schema_result = check_schema_drift(df, batch_id)
    report["checks"].append(schema_result)
    if not schema_result["passed"]:
        report["batch_passed"] = False
        report["failures"].append("schema_drift")
        send_dq_alert(batch_id, "SCHEMA_DRIFT", schema_result)
        return df, report

    # 2. Null rates — HARD FAIL
    null_result = check_null_rates(df, batch_id)
    report["checks"].append(null_result)
    if not null_result["passed"]:
        report["batch_passed"] = False
        report["failures"].append("null_rates")
        send_dq_alert(batch_id, "NULL_BREACH", null_result)

    # 3. Anomaly detection — SOFT FLAG (does not block batch)
    df, anomaly_result = check_anomalies(df, batch_id)
    report["checks"].append(anomaly_result)
    if anomaly_result.get("flagged_count", 0) > 0:
        send_dq_alert(batch_id, "ANOMALY_FLAG", anomaly_result, severity="WARNING")

    return df, report
