"""Athlete-table builders for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from datetime import date
from typing import Any

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.metadata import (
    build_metadata_payload,
    build_record_hash,
)
from napa_pipeline.bronze_to_silver.operations import PipelineContext
from napa_pipeline.bronze_to_silver.quality import build_reject_record
from napa_pipeline.bronze_to_silver.reconciliation import reconcile_table_counts
from napa_pipeline.bronze_to_silver.reference import (
    SilverBuildResult,
    _dedupe_exact_rows,
    _derive_active_flag,
    _normalize_source_keys,
    _resolve_duplicate_keys,
)
from napa_pipeline.bronze_to_silver.transforms import (
    normalize_domain_value,
    safe_cast_date,
    safe_cast_float,
    standardize_string,
)

DEFAULT_PLAYER_AGE_GROUPS = (
    {"label": "UNDER_18", "min_age": 0, "max_age": 17},
    {"label": "AGE_18_34", "min_age": 18, "max_age": 34},
    {"label": "AGE_35_49", "min_age": 35, "max_age": 49},
    {"label": "AGE_50_64", "min_age": 50, "max_age": 64},
    {"label": "AGE_65_PLUS", "min_age": 65},
)


def build_players(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    regions_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver players table from Bronze rows."""
    table_name = "players"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    country_domain = config.data["domains"]["country_code"]
    gender_domain = config.data["domains"]["gender"]
    dominant_hand_domain = config.data["domains"]["dominant_hand"]
    side_domain = config.data["domains"]["player_position"]
    player_age_groups = config.data["thresholds"].get("player_age_groups")
    region_index = _index_rows(regions_rows, "region_id")
    as_of_date = _resolve_release_as_of_date(monthly_batches_rows)

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        player_id = standardize_string(
            normalized.get("player_id") or normalized.get("id"),
            uppercase=False,
        )
        if not player_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="player_master",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="PLAYER_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="player_id could not be resolved.",
                )
            )
            continue

        candidate, candidate_reject = _build_player_candidate(
            normalized,
            player_id=player_id,
            region_index=region_index,
            country_domain=country_domain,
            gender_domain=gender_domain,
            dominant_hand_domain=dominant_hand_domain,
            side_domain=side_domain,
            player_age_groups=player_age_groups,
            as_of_date=as_of_date,
            context=context,
        )
        if candidate_reject is not None:
            rejects.append(candidate_reject)
            continue

        candidates.setdefault(player_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="player_master",
        target_table=table_name,
        duplicate_rule_id="PLAYER_DUPLICATE",
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


def build_player_registrations(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver player_registrations table from Bronze rows."""
    table_name = "player_registrations"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    players_index = _index_rows(players_rows, "player_id")
    batches_index = _index_rows(monthly_batches_rows, "batch_id")
    as_of_date = _resolve_release_as_of_date(monthly_batches_rows)

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        registration_id = standardize_string(
            normalized.get("registration_id") or normalized.get("id"),
            uppercase=False,
        )
        if not registration_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="player_registrations",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="REG_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="registration_id could not be resolved.",
                )
            )
            continue

        candidate, candidate_reject = _build_registration_candidate(
            normalized,
            registration_id=registration_id,
            players_index=players_index,
            batches_index=batches_index,
            as_of_date=as_of_date,
            context=context,
        )
        if candidate_reject is not None:
            rejects.append(candidate_reject)
            continue

        candidates.setdefault(registration_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="player_registrations",
        target_table=table_name,
        duplicate_rule_id="REG_DUPLICATE",
    )
    rejects.extend(duplicate_rejects)
    _assign_registration_sequence(accepted_rows)

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


def build_player_assessment_history(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver player_assessment_history table from Bronze rows."""
    table_name = "player_assessment_history"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    players_index = _index_rows(players_rows, "player_id")
    batches_index = _index_rows(monthly_batches_rows, "batch_id")
    as_of_date = _resolve_release_as_of_date(monthly_batches_rows)

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        assessment_id = standardize_string(
            normalized.get("assessment_id") or normalized.get("id"),
            uppercase=False,
        )
        if not assessment_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="player_assessment_history",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="ASSESS_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="assessment_id could not be resolved.",
                )
            )
            continue

        candidate, candidate_reject = _build_assessment_candidate(
            normalized,
            assessment_id=assessment_id,
            players_index=players_index,
            batches_index=batches_index,
            as_of_date=as_of_date,
            context=context,
        )
        if candidate_reject is not None:
            rejects.append(candidate_reject)
            continue

        candidates.setdefault(assessment_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="player_assessment_history",
        target_table=table_name,
        duplicate_rule_id="ASSESS_DUPLICATE",
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


def _build_player_candidate(
    normalized: dict[str, Any],
    *,
    player_id: str,
    region_index: dict[str, dict[str, Any]],
    country_domain: dict[str, Any],
    gender_domain: dict[str, Any],
    dominant_hand_domain: dict[str, Any],
    side_domain: dict[str, Any],
    player_age_groups: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
    as_of_date: date | None,
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    first_name = standardize_string(normalized.get("first_name"), uppercase=False)
    last_name = standardize_string(normalized.get("last_name"), uppercase=False)
    display_name = _derive_display_name(normalized, first_name, last_name)

    birth_date_raw = (
        normalized.get("birth_date")
        or normalized.get("date_of_birth")
        or normalized.get("dob")
    )
    try:
        birth_date = safe_cast_date(birth_date_raw)
    except Exception:
        return None, build_reject_record(
            context,
            source_table="player_master",
            target_table="players",
            source_business_key=player_id,
            reject_reason="INVALID_DATE",
            rule_id="PLAYER_005",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"Invalid birth date '{birth_date_raw}'.",
        )

    if birth_date is not None and as_of_date is not None and birth_date > as_of_date:
        return None, build_reject_record(
            context,
            source_table="player_master",
            target_table="players",
            source_business_key=player_id,
            reject_reason="INVALID_DATE_RANGE",
            rule_id="PLAYER_005",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="birth_date cannot be after the release as-of date.",
        )

    home_region_id = standardize_string(
        normalized.get("home_region_id") or normalized.get("region_id"),
        uppercase=False,
    )
    if home_region_id and home_region_id not in region_index:
        return None, build_reject_record(
            context,
            source_table="player_master",
            target_table="players",
            source_business_key=player_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="PLAYER_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"home_region_id '{home_region_id}' was not found in accepted regions.",
        )

    gender_raw = normalized.get("gender")
    gender = _normalize_optional_domain_value(gender_raw, gender_domain)
    if gender_raw not in (None, "") and gender is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="player_master",
            target_table="players",
            source_business_key=player_id,
            rule_id="PLAYER_003",
            detail=f"Invalid gender value '{gender_raw}'.",
            source_record=normalized,
        )

    dominant_hand_raw = normalized.get("dominant_hand") or normalized.get("handedness")
    dominant_hand = _normalize_optional_domain_value(dominant_hand_raw, dominant_hand_domain)
    if dominant_hand_raw not in (None, "") and dominant_hand is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="player_master",
            target_table="players",
            source_business_key=player_id,
            rule_id="PLAYER_004",
            detail=f"Invalid dominant_hand value '{dominant_hand_raw}'.",
            source_record=normalized,
        )

    preferred_side_raw = normalized.get("preferred_side") or normalized.get("preferred_position")
    preferred_side = _normalize_optional_domain_value(preferred_side_raw, side_domain)
    if preferred_side_raw not in (None, "") and preferred_side is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="player_master",
            target_table="players",
            source_business_key=player_id,
            rule_id="PLAYER_006",
            detail=f"Invalid preferred_side value '{preferred_side_raw}'.",
            source_record=normalized,
        )

    country_raw = normalized.get("country_code") or normalized.get("country")
    country_code = _normalize_optional_domain_value(country_raw, country_domain)
    if country_raw not in (None, "") and country_code is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="player_master",
            target_table="players",
            source_business_key=player_id,
            rule_id="PLAYER_007",
            detail=f"Invalid country value '{country_raw}'.",
            source_record=normalized,
        )

    rating, rating_reject = _safe_optional_float(
        normalized.get("rating") or normalized.get("player_rating"),
        context=context,
        source_table="player_master",
        target_table="players",
        source_business_key=player_id,
        rule_id="PLAYER_008",
        source_record=normalized,
        field_name="rating",
    )
    if rating_reject is not None:
        return None, rating_reject

    confidence, confidence_reject = _safe_optional_float(
        normalized.get("rating_confidence") or normalized.get("confidence"),
        context=context,
        source_table="player_master",
        target_table="players",
        source_business_key=player_id,
        rule_id="PLAYER_009",
        source_record=normalized,
        field_name="rating_confidence",
    )
    if confidence_reject is not None:
        return None, confidence_reject

    region_row = region_index.get(home_region_id) if home_region_id else None
    age = _calculate_age(birth_date, as_of_date)

    candidate = {
        "player_id": player_id,
        "player_sk": build_record_hash({"player_id": player_id}, ["player_id"]),
        "first_name": first_name,
        "last_name": last_name,
        "display_name": display_name,
        "birth_date": birth_date,
        "gender": gender,
        "dominant_hand": dominant_hand,
        "preferred_side": preferred_side,
        "home_region_id": home_region_id,
        "home_region_sk": region_row.get("region_sk") if region_row else None,
        "country_code": country_code,
        "active_flag": _derive_active_flag(normalized),
        "age": age,
        "age_group": _derive_age_group(
            age,
            config_values=player_age_groups,
        ),
        "rating": rating,
        "rating_confidence": confidence,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "player_master",
            build_record_hash(
                {
                    "player_id": player_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "birth_date": birth_date,
                    "home_region_id": home_region_id,
                },
                ["player_id", "first_name", "last_name", "birth_date", "home_region_id"],
            ),
        )
    )
    return candidate, None


def _build_registration_candidate(
    normalized: dict[str, Any],
    *,
    registration_id: str,
    players_index: dict[str, dict[str, Any]],
    batches_index: dict[str, dict[str, Any]],
    as_of_date: date | None,
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    player_id = standardize_string(normalized.get("player_id"), uppercase=False)
    if not player_id or player_id not in players_index:
        return None, build_reject_record(
            context,
            source_table="player_registrations",
            target_table="player_registrations",
            source_business_key=registration_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="REG_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"player_id '{player_id}' was not found in accepted players.",
        )

    batch_id = standardize_string(
        normalized.get("batch_id") or normalized.get("monthly_batch_id"),
        uppercase=False,
    )
    if batch_id and batch_id not in batches_index:
        return None, build_reject_record(
            context,
            source_table="player_registrations",
            target_table="player_registrations",
            source_business_key=registration_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="REG_003",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"batch_id '{batch_id}' was not found in accepted monthly_batches.",
        )

    registration_date, date_reject = _safe_optional_date(
        normalized.get("registration_date"),
        context=context,
        source_table="player_registrations",
        target_table="player_registrations",
        source_business_key=registration_id,
        rule_id="REG_004",
        source_record=normalized,
        field_name="registration_date",
    )
    if date_reject is not None:
        return None, date_reject

    effective_start_date, start_reject = _safe_optional_date(
        normalized.get("effective_start_date") or normalized.get("start_date"),
        context=context,
        source_table="player_registrations",
        target_table="player_registrations",
        source_business_key=registration_id,
        rule_id="REG_005",
        source_record=normalized,
        field_name="effective_start_date",
    )
    if start_reject is not None:
        return None, start_reject

    effective_end_date, end_reject = _safe_optional_date(
        normalized.get("effective_end_date") or normalized.get("end_date"),
        context=context,
        source_table="player_registrations",
        target_table="player_registrations",
        source_business_key=registration_id,
        rule_id="REG_006",
        source_record=normalized,
        field_name="effective_end_date",
    )
    if end_reject is not None:
        return None, end_reject

    if (
        effective_start_date is not None
        and effective_end_date is not None
        and effective_end_date < effective_start_date
    ):
        return None, build_reject_record(
            context,
            source_table="player_registrations",
            target_table="player_registrations",
            source_business_key=registration_id,
            reject_reason="INVALID_DATE_RANGE",
            rule_id="REG_007",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="effective_end_date cannot be before effective_start_date.",
        )

    player_row = players_index[player_id]
    batch_row = batches_index.get(batch_id) if batch_id else None
    current_registration_flag = _is_current_period(
        effective_start_date=effective_start_date,
        effective_end_date=effective_end_date,
        as_of_date=as_of_date or registration_date,
    )

    candidate = {
        "registration_id": registration_id,
        "registration_sk": build_record_hash(
            {"registration_id": registration_id},
            ["registration_id"],
        ),
        "player_id": player_id,
        "player_sk": player_row["player_sk"],
        "batch_id": batch_id,
        "batch_sk": batch_row.get("batch_sk") if batch_row else None,
        "registration_date": registration_date,
        "registration_type": standardize_string(
            normalized.get("registration_type"),
            uppercase=True,
        ),
        "registration_status": standardize_string(
            normalized.get("registration_status") or normalized.get("status"),
            uppercase=True,
        ),
        "effective_start_date": effective_start_date,
        "effective_end_date": effective_end_date,
        "current_registration_flag": current_registration_flag,
        "registration_duration_days": _duration_days(
            effective_start_date,
            effective_end_date,
        ),
        "registration_sequence": None,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "player_registrations",
            build_record_hash(
                {
                    "registration_id": registration_id,
                    "player_id": player_id,
                    "batch_id": batch_id,
                    "registration_date": registration_date,
                    "effective_start_date": effective_start_date,
                    "effective_end_date": effective_end_date,
                },
                [
                    "registration_id",
                    "player_id",
                    "batch_id",
                    "registration_date",
                    "effective_start_date",
                    "effective_end_date",
                ],
            ),
        )
    )
    return candidate, None


def _build_assessment_candidate(
    normalized: dict[str, Any],
    *,
    assessment_id: str,
    players_index: dict[str, dict[str, Any]],
    batches_index: dict[str, dict[str, Any]],
    as_of_date: date | None,
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    player_id = standardize_string(normalized.get("player_id"), uppercase=False)
    if not player_id or player_id not in players_index:
        return None, build_reject_record(
            context,
            source_table="player_assessment_history",
            target_table="player_assessment_history",
            source_business_key=assessment_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="ASSESS_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"player_id '{player_id}' was not found in accepted players.",
        )

    batch_id = standardize_string(
        normalized.get("batch_id") or normalized.get("monthly_batch_id"),
        uppercase=False,
    )
    if batch_id and batch_id not in batches_index:
        return None, build_reject_record(
            context,
            source_table="player_assessment_history",
            target_table="player_assessment_history",
            source_business_key=assessment_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="ASSESS_003",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"batch_id '{batch_id}' was not found in accepted monthly_batches.",
        )

    assessment_date, date_reject = _safe_optional_date(
        normalized.get("assessment_date"),
        context=context,
        source_table="player_assessment_history",
        target_table="player_assessment_history",
        source_business_key=assessment_id,
        rule_id="ASSESS_004",
        source_record=normalized,
        field_name="assessment_date",
    )
    if date_reject is not None:
        return None, date_reject

    if assessment_date is not None and as_of_date is not None and assessment_date > as_of_date:
        return None, build_reject_record(
            context,
            source_table="player_assessment_history",
            target_table="player_assessment_history",
            source_business_key=assessment_id,
            reject_reason="INVALID_DATE_RANGE",
            rule_id="ASSESS_004",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="assessment_date cannot be after the release as-of date.",
        )

    assessment_value, value_reject = _safe_optional_float(
        normalized.get("assessment_value") or normalized.get("value"),
        context=context,
        source_table="player_assessment_history",
        target_table="player_assessment_history",
        source_business_key=assessment_id,
        rule_id="ASSESS_005",
        source_record=normalized,
        field_name="assessment_value",
    )
    if value_reject is not None:
        return None, value_reject

    assessment_confidence, confidence_reject = _safe_optional_float(
        normalized.get("assessment_confidence") or normalized.get("confidence"),
        context=context,
        source_table="player_assessment_history",
        target_table="player_assessment_history",
        source_business_key=assessment_id,
        rule_id="ASSESS_006",
        source_record=normalized,
        field_name="assessment_confidence",
    )
    if confidence_reject is not None:
        return None, confidence_reject

    player_row = players_index[player_id]
    batch_row = batches_index.get(batch_id) if batch_id else None
    candidate = {
        "assessment_id": assessment_id,
        "assessment_sk": build_record_hash(
            {"assessment_id": assessment_id},
            ["assessment_id"],
        ),
        "player_id": player_id,
        "player_sk": player_row["player_sk"],
        "batch_id": batch_id,
        "batch_sk": batch_row.get("batch_sk") if batch_row else None,
        "assessment_date": assessment_date,
        "assessment_type": standardize_string(
            normalized.get("assessment_type"),
            uppercase=True,
        ),
        "assessment_value": assessment_value,
        "assessment_confidence": assessment_confidence,
        "assessor_source": standardize_string(
            normalized.get("assessor_source"),
            uppercase=False,
        ),
    }
    candidate.update(
        build_metadata_payload(
            context,
            "player_assessment_history",
            build_record_hash(
                {
                    "assessment_id": assessment_id,
                    "player_id": player_id,
                    "batch_id": batch_id,
                    "assessment_date": assessment_date,
                    "assessment_type": candidate["assessment_type"],
                    "assessment_value": assessment_value,
                },
                [
                    "assessment_id",
                    "player_id",
                    "batch_id",
                    "assessment_date",
                    "assessment_type",
                    "assessment_value",
                ],
            ),
        )
    )
    return candidate, None


def _derive_display_name(
    normalized: dict[str, Any],
    first_name: str | None,
    last_name: str | None,
) -> str | None:
    explicit_name = standardize_string(
        normalized.get("display_name") or normalized.get("full_name"),
        uppercase=False,
    )
    if explicit_name:
        return explicit_name
    parts = [part for part in (first_name, last_name) if part]
    return " ".join(parts) if parts else None


def _index_rows(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    key_name: str,
) -> dict[str, dict[str, Any]]:
    return {
        str(row[key_name]): row
        for row in rows
        if row.get(key_name) not in (None, "")
    }


def _resolve_release_as_of_date(
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> date | None:
    batch_dates = [
        row["batch_date"]
        for row in monthly_batches_rows
        if isinstance(row.get("batch_date"), date)
    ]
    return max(batch_dates) if batch_dates else None


def _normalize_optional_domain_value(
    value: Any,
    domain_config: dict[str, Any],
) -> str | None:
    if value in (None, ""):
        return None
    return normalize_domain_value(value, domain_config)


def _safe_optional_date(
    value: Any,
    *,
    context: PipelineContext,
    source_table: str,
    target_table: str,
    source_business_key: str,
    rule_id: str,
    source_record: dict[str, Any],
    field_name: str,
) -> tuple[date | None, dict[str, Any] | None]:
    try:
        return safe_cast_date(value), None
    except Exception:
        return None, build_reject_record(
            context,
            source_table=source_table,
            target_table=target_table,
            source_business_key=source_business_key,
            reject_reason="INVALID_DATE",
            rule_id=rule_id,
            rule_severity="ERROR",
            source_record=source_record,
            reject_reason_detail=f"Invalid {field_name} value '{value}'.",
        )


def _safe_optional_float(
    value: Any,
    *,
    context: PipelineContext,
    source_table: str,
    target_table: str,
    source_business_key: str,
    rule_id: str,
    source_record: dict[str, Any],
    field_name: str,
) -> tuple[float | None, dict[str, Any] | None]:
    try:
        return safe_cast_float(value), None
    except Exception:
        return None, build_reject_record(
            context,
            source_table=source_table,
            target_table=target_table,
            source_business_key=source_business_key,
            reject_reason="INVALID_DATA_TYPE",
            rule_id=rule_id,
            rule_severity="ERROR",
            source_record=source_record,
            reject_reason_detail=f"Invalid {field_name} value '{value}'.",
        )


def _invalid_domain_reject(
    *,
    context: PipelineContext,
    source_table: str,
    target_table: str,
    source_business_key: str,
    rule_id: str,
    detail: str,
    source_record: dict[str, Any],
) -> dict[str, Any]:
    return build_reject_record(
        context,
        source_table=source_table,
        target_table=target_table,
        source_business_key=source_business_key,
        reject_reason="INVALID_DOMAIN_VALUE",
        rule_id=rule_id,
        rule_severity="ERROR",
        source_record=source_record,
        reject_reason_detail=detail,
    )


def _calculate_age(
    birth_date: date | None,
    as_of_date: date | None,
) -> int | None:
    if birth_date is None or as_of_date is None:
        return None
    years = as_of_date.year - birth_date.year
    before_birthday = (as_of_date.month, as_of_date.day) < (birth_date.month, birth_date.day)
    return years - 1 if before_birthday else years


def _derive_age_group(
    age: int | None,
    *,
    config_values: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None,
) -> str | None:
    if age is None:
        return None
    age_groups = config_values or DEFAULT_PLAYER_AGE_GROUPS
    for group in age_groups:
        minimum_age = group.get("min_age")
        maximum_age = group.get("max_age")
        if minimum_age is not None and age < int(minimum_age):
            continue
        if maximum_age is not None and age > int(maximum_age):
            continue
        label = group.get("label")
        if label is not None:
            return str(label)
    return None


def _is_current_period(
    *,
    effective_start_date: date | None,
    effective_end_date: date | None,
    as_of_date: date | None,
) -> bool | None:
    if as_of_date is None:
        return None
    if effective_start_date is not None and effective_start_date > as_of_date:
        return False
    if effective_end_date is not None and effective_end_date < as_of_date:
        return False
    return True


def _duration_days(
    start_date: date | None,
    end_date: date | None,
) -> int | None:
    if start_date is None or end_date is None:
        return None
    return (end_date - start_date).days


def _assign_registration_sequence(accepted_rows: list[dict[str, Any]]) -> None:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in accepted_rows:
        grouped.setdefault(str(row["player_id"]), []).append(row)

    for player_rows in grouped.values():
        ordered = sorted(
            player_rows,
            key=lambda row: (
                row.get("registration_date") or date.min,
                row.get("effective_start_date") or date.min,
                str(row["registration_id"]),
            ),
        )
        for sequence, row in enumerate(ordered, start=1):
            row["registration_sequence"] = sequence
