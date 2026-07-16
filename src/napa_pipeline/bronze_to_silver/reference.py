"""Reference-table builders for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.metadata import (
    build_metadata_payload,
    build_record_hash,
)
from napa_pipeline.bronze_to_silver.operations import PipelineContext
from napa_pipeline.bronze_to_silver.quality import build_reject_record
from napa_pipeline.bronze_to_silver.reconciliation import (
    ReconciliationSummary,
    reconcile_table_counts,
)
from napa_pipeline.bronze_to_silver.transforms import (
    normalize_domain_value,
    safe_cast_date,
    safe_cast_int,
    standardize_string,
    to_snake_case,
)


@dataclass(frozen=True)
class SilverBuildResult:
    """Accepted rows, rejects, and reconciliation evidence for one Silver build."""

    target_table: str
    accepted_rows: tuple[dict[str, Any], ...]
    rejected_rows: tuple[dict[str, Any], ...]
    exact_duplicate_count: int
    business_key_duplicate_count: int
    warning_count: int
    reconciliation: ReconciliationSummary


def build_monthly_batches(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
) -> SilverBuildResult:
    """Build the Silver monthly_batches table from Bronze rows."""
    table_name = "monthly_batches"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        batch_id = standardize_string(normalized.get("id"), uppercase=False)
        if not batch_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="monthly_batches",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="BATCH_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="batch_id could not be resolved from id.",
                )
            )
            continue

        candidate, candidate_reject = _build_monthly_batch_candidate(
            normalized,
            batch_id=batch_id,
            context=context,
        )
        if candidate_reject is not None:
            rejects.append(candidate_reject)
            continue

        candidates.setdefault(batch_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="monthly_batches",
        target_table=table_name,
        duplicate_rule_id="BATCH_DUPLICATE",
    )
    rejects.extend(duplicate_rejects)

    reconciliation = reconcile_table_counts(
        bronze_row_count=len(source_rows),
        exact_duplicate_count=exact_duplicate_count,
        business_key_loser_count=business_key_duplicate_count,
        rejected_row_count=len(rejects),
        accepted_row_count=len(accepted_rows),
    )

    return SilverBuildResult(
        target_table=table_name,
        accepted_rows=tuple(accepted_rows),
        rejected_rows=tuple(rejects),
        exact_duplicate_count=exact_duplicate_count,
        business_key_duplicate_count=business_key_duplicate_count,
        warning_count=warning_count,
        reconciliation=reconciliation,
    )


def build_regions(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
) -> SilverBuildResult:
    """Build the Silver regions table from Bronze rows."""
    table_name = "regions"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    country_domain = config.data["domains"]["country_code"]

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        region_id = standardize_string(normalized.get("id"), uppercase=False)
        if not region_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="regions",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="REGION_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="region_id could not be resolved from id.",
                )
            )
            continue

        candidate, candidate_reject = _build_region_candidate(
            normalized,
            region_id=region_id,
            country_domain=country_domain,
            context=context,
        )
        if candidate_reject is not None:
            rejects.append(candidate_reject)
            continue

        candidates.setdefault(region_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="regions",
        target_table=table_name,
        duplicate_rule_id="REGION_DUPLICATE",
    )
    rejects.extend(duplicate_rejects)

    reconciliation = reconcile_table_counts(
        bronze_row_count=len(source_rows),
        exact_duplicate_count=exact_duplicate_count,
        business_key_loser_count=business_key_duplicate_count,
        rejected_row_count=len(rejects),
        accepted_row_count=len(accepted_rows),
    )

    return SilverBuildResult(
        target_table=table_name,
        accepted_rows=tuple(accepted_rows),
        rejected_rows=tuple(rejects),
        exact_duplicate_count=exact_duplicate_count,
        business_key_duplicate_count=business_key_duplicate_count,
        warning_count=warning_count,
        reconciliation=reconciliation,
    )


def _build_monthly_batch_candidate(
    normalized: dict[str, Any],
    *,
    batch_id: str,
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    batch_date = None
    batch_date_raw = (
        normalized.get("batch_date")
        or normalized.get("release_date")
        or normalized.get("date")
    )
    if batch_date_raw not in (None, ""):
        try:
            batch_date = safe_cast_date(batch_date_raw)
        except Exception:
            return None, build_reject_record(
                context,
                source_table="monthly_batches",
                target_table="monthly_batches",
                source_business_key=batch_id,
                reject_reason="INVALID_DATE",
                rule_id="BATCH_002",
                rule_severity="ERROR",
                source_record=normalized,
                reject_reason_detail=f"Invalid batch date '{batch_date_raw}'.",
            )

    batch_sequence = None
    batch_sequence_raw = normalized.get("batch_sequence") or normalized.get("sequence")
    if batch_sequence_raw not in (None, ""):
        try:
            batch_sequence = safe_cast_int(batch_sequence_raw)
        except Exception:
            return None, build_reject_record(
                context,
                source_table="monthly_batches",
                target_table="monthly_batches",
                source_business_key=batch_id,
                reject_reason="VALUE_OUT_OF_RANGE",
                rule_id="BATCH_003",
                rule_severity="ERROR",
                source_record=normalized,
                reject_reason_detail=f"Invalid batch sequence '{batch_sequence_raw}'.",
            )
        if batch_sequence is not None and batch_sequence < 0:
            return None, build_reject_record(
                context,
                source_table="monthly_batches",
                target_table="monthly_batches",
                source_business_key=batch_id,
                reject_reason="VALUE_OUT_OF_RANGE",
                rule_id="BATCH_003",
                rule_severity="ERROR",
                source_record=normalized,
                reject_reason_detail="Batch sequence must be non-negative.",
            )

    batch_type = standardize_string(normalized.get("batch_type"), uppercase=True)
    batch_status = standardize_string(
        normalized.get("batch_status") or normalized.get("status"),
        uppercase=True,
    )

    candidate = {
        "batch_id": batch_id,
        "batch_sk": build_record_hash({"batch_id": batch_id}, ["batch_id"]),
        "batch_sequence": batch_sequence,
        "batch_date": batch_date,
        "batch_year": batch_date.year if isinstance(batch_date, date) else None,
        "batch_month": batch_date.month if isinstance(batch_date, date) else None,
        "batch_quarter": _derive_quarter(batch_date) if isinstance(batch_date, date) else None,
        "batch_type": batch_type,
        "batch_status": batch_status,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "monthly_batches",
            build_record_hash(
                {
                    "batch_id": batch_id,
                    "batch_sequence": batch_sequence,
                    "batch_date": batch_date,
                },
                ["batch_id", "batch_sequence", "batch_date"],
            ),
        )
    )
    return candidate, None


def _build_region_candidate(
    normalized: dict[str, Any],
    *,
    region_id: str,
    country_domain: dict[str, Any],
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    region_name = standardize_string(
        normalized.get("region_name") or normalized.get("name"),
        uppercase=False,
    )
    if not region_name:
        return None, build_reject_record(
            context,
            source_table="regions",
            target_table="regions",
            source_business_key=region_id,
            reject_reason="MISSING_REQUIRED_COLUMN",
            rule_id="REGION_002",
            rule_severity="CRITICAL",
            source_record=normalized,
            reject_reason_detail="region_name could not be resolved.",
        )

    country_value = (
        normalized.get("country_code")
        or normalized.get("country")
        or normalized.get("country_name")
    )
    country_code = normalize_domain_value(country_value, country_domain)
    if not country_code:
        return None, build_reject_record(
            context,
            source_table="regions",
            target_table="regions",
            source_business_key=region_id,
            reject_reason="INVALID_DOMAIN_VALUE",
            rule_id="REGION_003",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"Invalid country value '{country_value}'.",
        )

    province_state = standardize_string(
        normalized.get("province_state")
        or normalized.get("province")
        or normalized.get("state"),
        uppercase=True,
    )
    active_flag = _derive_active_flag(normalized)

    candidate = {
        "region_id": region_id,
        "region_sk": build_record_hash({"region_id": region_id}, ["region_id"]),
        "region_name": region_name,
        "province_state": province_state,
        "country_code": country_code,
        "active_flag": active_flag,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "regions",
            build_record_hash(
                {
                    "region_id": region_id,
                    "region_name": region_name,
                    "country_code": country_code,
                },
                ["region_id", "region_name", "country_code"],
            ),
        )
    )
    return candidate, None


def _normalize_source_keys(source_row: dict[str, Any]) -> dict[str, Any]:
    return {to_snake_case(key): value for key, value in source_row.items()}


def _dedupe_exact_rows(
    source_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    seen: set[tuple[tuple[str, Any], ...]] = set()
    deduped: list[dict[str, Any]] = []
    duplicate_count = 0
    for row in source_rows:
        normalized = _normalize_source_keys(row)
        signature = tuple(sorted(normalized.items()))
        if signature in seen:
            duplicate_count += 1
            continue
        seen.add(signature)
        deduped.append(row)
    return deduped, duplicate_count


def _resolve_duplicate_keys(
    candidates: dict[str, list[dict[str, Any]]],
    *,
    context: PipelineContext,
    source_table: str,
    target_table: str,
    duplicate_rule_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    accepted_rows: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    duplicate_count = 0

    for business_key, records in candidates.items():
        if len(records) == 1:
            accepted_rows.append(records[0])
            continue

        sorted_records = sorted(records, key=_duplicate_priority_key)
        accepted_rows.append(sorted_records[0])
        for losing_record in sorted_records[1:]:
            duplicate_count += 1
            rejects.append(
                build_reject_record(
                    context,
                    source_table=source_table,
                    target_table=target_table,
                    source_business_key=business_key,
                    reject_reason="DUPLICATE_BUSINESS_KEY",
                    rule_id=duplicate_rule_id,
                    rule_severity="ERROR",
                    source_record=losing_record,
                    reject_reason_detail="Duplicate business key lost deterministic tie-break.",
                )
            )

    return accepted_rows, rejects, duplicate_count


def _duplicate_priority_key(record: dict[str, Any]) -> tuple[int, str]:
    completeness = sum(1 for value in record.values() if value not in (None, ""))
    return (-completeness, str(record.get("_record_hash", "")))


def _derive_quarter(batch_date: date) -> int:
    return ((batch_date.month - 1) // 3) + 1


def _derive_active_flag(normalized: dict[str, Any]) -> bool | None:
    explicit_flag = normalized.get("active_flag")
    if explicit_flag is not None:
        if isinstance(explicit_flag, bool):
            return explicit_flag
        text = standardize_string(explicit_flag, uppercase=True)
        if text in {"TRUE", "T", "YES", "Y", "1", "ACTIVE"}:
            return True
        if text in {"FALSE", "F", "NO", "N", "0", "INACTIVE"}:
            return False

    status = standardize_string(normalized.get("status"), uppercase=True)
    if status == "ACTIVE":
        return True
    if status == "INACTIVE":
        return False
    return None
