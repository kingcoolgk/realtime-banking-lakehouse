"""
dq/dq_alert.py
----------------
Alerting module for DQ failures.
Supports logging + extensible hooks for Slack / email / PagerDuty, plus an
agentic diagnosis layer (dq/agentic_diagnosis.py) for ERROR-severity alerts.
"""

import json
import logging
import os
from datetime import datetime

from dq import agentic_diagnosis

logger = logging.getLogger(__name__)

# Only run the agent on hard failures — anomaly WARNINGs fire far more often
# and don't need a Claude call each time; keeps API cost predictable.
AGENT_SEVERITIES = {"ERROR"}


def send_dq_alert(batch_id: int, alert_type: str,
                   details: dict, severity: str = "ERROR"):
    """
    Send a DQ alert. Logs to console, runs agentic diagnosis on hard
    failures, and pushes to Slack if configured.

    Args:
        batch_id   : Spark micro-batch ID
        alert_type : e.g. SCHEMA_DRIFT, NULL_BREACH, ANOMALY_FLAG
        details    : dict with check-specific metadata
        severity   : ERROR (hard fail) | WARNING (soft flag)
    """
    alert_payload = {
        "timestamp":  datetime.utcnow().isoformat(),
        "batch_id":   batch_id,
        "alert_type": alert_type,
        "severity":   severity,
        "details":    details,
    }

    log_msg = f"[DQ ALERT | {severity}] {alert_type} — Batch {batch_id} | {json.dumps(details)}"

    if severity == "ERROR":
        logger.error(log_msg)
    else:
        logger.warning(log_msg)

    slack_message = log_msg  # overwritten with the agent's summary if diagnosis succeeds

    # Agentic diagnosis is handled by the Airflow failure branch.
# Keep Spark DQ processing independent of external LLM API calls.
    if (
        severity in AGENT_SEVERITIES
        and os.getenv("INLINE_AGENTIC_DQ", "false").lower() == "true"
    ):
        diagnosis = agentic_diagnosis.diagnose(batch_id, alert_type, details)
        if diagnosis:
            logger.info(
                f"Batch {batch_id} | Agent diagnosis: {json.dumps(diagnosis)}"
            )
            slack_message = agentic_diagnosis.format_for_slack(
                batch_id, alert_type, diagnosis
            )
            alert_payload["diagnosis"] = diagnosis

    # ── Extension point ───────────────────────────────────────────────────────
    slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if slack_webhook:
        import requests
        try:
            requests.post(slack_webhook, json={"text": slack_message}, timeout=5)
        except Exception as e:
            logger.warning(f"Batch {batch_id} | Slack notification failed: {e}")

    # Optional: AWS SNS instead of/alongside Slack
    # import boto3
    # sns = boto3.client("sns", region_name="ap-south-1")
    # sns.publish(TopicArn="arn:aws:sns:...", Message=json.dumps(alert_payload))
