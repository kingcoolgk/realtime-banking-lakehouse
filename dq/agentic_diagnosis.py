"""
dq/agentic_diagnosis.py
--------------------------
Agentic data-quality layer.

Hooks into dq/dq_alert.py's send_dq_alert() at the ERROR severity extension
point. Instead of just logging a raw failure dict, this sends the DQ
report to Claude and gets back a structured root-cause hypothesis and
suggested next step — the kind of triage a data engineer would otherwise
do by hand by grepping logs.

Deliberately NOT auto-remediating: this agent detects and diagnoses, a
human approves the fix. That's the honest scope for a personal project,
and it mirrors how production agentic systems are actually being run —
autonomous triage, human-approved action.

Uses Haiku rather than a larger model: alert triage is a small, frequent,
latency-sensitive task, and Haiku's structured-output quality is enough
for this — a deliberate cost/latency tradeoff, not a default.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"

_client: Optional[anthropic.Anthropic] = None


def _get_client() -> Optional[anthropic.Anthropic]:
    """Lazily construct the Anthropic client. Returns None (and logs a
    warning once) if ANTHROPIC_API_KEY isn't set, so the pipeline degrades
    to plain logging instead of crashing when the key is missing."""
    global _client
    if _client is not None:
        return _client

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — agentic diagnosis disabled, falling back to plain alerts.")
        return None

    _client = anthropic.Anthropic(api_key=api_key)
    return _client


DIAGNOSIS_SYSTEM_PROMPT = """You are a data-quality triage assistant for a real-time \
credit card transaction pipeline (Kafka -> PySpark Structured Streaming -> S3/Glue lakehouse).

You will be given a DQ check failure as JSON. Respond with ONLY a JSON object \
(no markdown, no preamble) with exactly these keys:
- "likely_cause": one or two sentences, your best hypothesis for what caused this
- "confidence": "low" | "medium" | "high"
- "suggested_action": one concrete next step a data engineer should take
- "is_actionable_now": true if this looks like something automatable in future \
  (e.g. a known schema change), false if it needs human investigation
"""


def diagnose(batch_id: int, alert_type: str, details: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Send a DQ failure to Claude for root-cause diagnosis.
    Returns the parsed diagnosis dict, or None if diagnosis wasn't available
    (missing API key or an API error) — callers should treat None as
    "fall back to the plain alert" rather than raise.
    """
    client = _get_client()
    if client is None:
        return None

    payload = {"batch_id": batch_id, "alert_type": alert_type, "details": details}

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=300,
            system=DIAGNOSIS_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": json.dumps(payload)}],
        )
        raw_text = response.content[0].text.strip()
        diagnosis = json.loads(raw_text)
        diagnosis["diagnosed_at"] = datetime.now(timezone.utc).isoformat()
        diagnosis["model"] = MODEL
        return diagnosis

    except json.JSONDecodeError:
        logger.error(f"Batch {batch_id} | Agent returned non-JSON output, skipping diagnosis: {raw_text[:200]}")
        return None
    except Exception as e:
        logger.error(f"Batch {batch_id} | Agentic diagnosis failed: {e}")
        return None


def format_for_slack(batch_id: int, alert_type: str, diagnosis: Dict[str, Any]) -> str:
    confidence_emoji = {"high": "\U0001F534", "medium": "\U0001F7E1", "low": "\u26AA"}.get(
        diagnosis.get("confidence", "low"), "\u26AA"
    )
    return (
        f"*DQ Alert — {alert_type}* (batch {batch_id})\n"
        f"{confidence_emoji} *Confidence:* {diagnosis.get('confidence')}\n"
        f"*Likely cause:* {diagnosis.get('likely_cause')}\n"
        f"*Suggested action:* {diagnosis.get('suggested_action')}"
    )
