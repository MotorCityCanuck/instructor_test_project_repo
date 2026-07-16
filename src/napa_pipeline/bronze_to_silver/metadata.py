"""Metadata helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Iterable

from napa_pipeline.bronze_to_silver.operations import PipelineContext


STANDARD_METADATA_COLUMNS = (
    "_pipeline_run_id",
    "_pipeline_version",
    "_source_dataset",
    "_source_table",
    "_load_ts",
    "_record_hash",
    "_data_quality_status",
)


def utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def build_metadata_payload(
    context: PipelineContext,
    source_table: str,
    record_hash: str,
    quality_status: str = "ACCEPTED",
    load_ts: datetime | None = None,
) -> dict[str, Any]:
    """Build the standard metadata payload for one accepted Silver record."""
    return {
        "_pipeline_run_id": context.pipeline_run_id,
        "_pipeline_version": context.pipeline_version,
        "_source_dataset": context.release_name,
        "_source_table": source_table,
        "_load_ts": load_ts or utc_now(),
        "_record_hash": record_hash,
        "_data_quality_status": quality_status,
    }


def build_record_hash(
    record: dict[str, Any],
    hash_columns: Iterable[str],
) -> str:
    """Build a deterministic SHA-256 hash for selected business columns."""
    canonical_values = []
    for column_name in hash_columns:
        value = record.get(column_name)
        canonical_values.append(_canonicalize_value(value))
    canonical = "|".join(canonical_values)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_source_record_json(record: dict[str, Any]) -> str:
    """Return a stable JSON serialization for reject evidence."""
    return json.dumps(
        record,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )


def _canonicalize_value(value: Any) -> str:
    if value is None:
        return "<NULL>"
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.isoformat()
    return str(value)
