"""Quality and reject helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

import hashlib
from typing import Any

from napa_pipeline.bronze_to_silver.metadata import build_source_record_json, utc_now
from napa_pipeline.bronze_to_silver.operations import PipelineContext


SEVERITY_TO_QUALITY_STATUS = {
    "CRITICAL": "REJECTED",
    "ERROR": "REJECTED",
    "WARNING": "WARNING",
    "INFO": "INFO",
}


def resolve_quality_status(severity: str) -> str:
    """Map a rule severity to a record-quality status."""
    normalized = severity.strip().upper()
    return SEVERITY_TO_QUALITY_STATUS.get(normalized, "UNKNOWN")


def build_reject_record(
    context: PipelineContext,
    source_table: str,
    target_table: str,
    source_business_key: str,
    reject_reason: str,
    rule_id: str,
    rule_severity: str,
    source_record: dict[str, Any],
    reject_reason_detail: str | None = None,
) -> dict[str, Any]:
    """Build a reject evidence record for one failed source row."""
    source_record_json = build_source_record_json(source_record)
    load_ts = utc_now()
    record_hash = hashlib.sha256(source_record_json.encode("utf-8")).hexdigest()
    return {
        "source_table": source_table,
        "target_table": target_table,
        "source_business_key": source_business_key,
        "reject_reason": reject_reason,
        "reject_reason_code": reject_reason,
        "reject_reason_detail": reject_reason_detail,
        "rule_id": rule_id,
        "rule_severity": rule_severity,
        "pipeline_run_id": context.pipeline_run_id,
        "_pipeline_run_id": context.pipeline_run_id,
        "_source_dataset": context.release_name,
        "load_ts": load_ts,
        "_load_ts": load_ts,
        "source_record_json": source_record_json,
        "_record_hash": record_hash,
    }


def calculate_quality_score(
    warning_count: int,
    warning_deduction: int,
) -> int:
    """Calculate a bounded operational data-quality score."""
    score = 100 - (warning_count * warning_deduction)
    return max(score, 0)
