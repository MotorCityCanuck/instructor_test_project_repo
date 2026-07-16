"""Organization and partnership table builders for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from datetime import date
from typing import Any

from napa_pipeline.bronze_to_silver.athlete import (
    _duration_days,
    _index_rows,
    _invalid_domain_reject,
    _is_current_period,
    _normalize_optional_domain_value,
    _resolve_release_as_of_date,
    _safe_optional_date,
)
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
    standardize_string,
)


def build_clubs(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    regions_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> SilverBuildResult:
    """Build the Silver clubs table from Bronze rows."""
    table_name = "clubs"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    region_index = _index_rows(regions_rows, "region_id")
    country_domain = config.data["domains"]["country_code"]

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        club_id = standardize_string(normalized.get("club_id") or normalized.get("id"), uppercase=False)
        if not club_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="clubs",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="CLUB_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="club_id could not be resolved.",
                )
            )
            continue

        candidate, reject = _build_club_candidate(
            normalized,
            club_id=club_id,
            region_index=region_index,
            country_domain=country_domain,
            context=context,
        )
        if reject is not None:
            rejects.append(reject)
            continue

        candidates.setdefault(club_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="clubs",
        target_table=table_name,
        duplicate_rule_id="CLUB_DUPLICATE",
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


def build_teams(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver teams table from Bronze rows."""
    table_name = "teams"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    team_type_domain = config.data["domains"]["team_type"]
    team_status_domain = config.data["domains"]["team_status"]
    country_domain = config.data["domains"]["country_code"]
    as_of_date = _resolve_release_as_of_date(monthly_batches_rows)

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        team_id = standardize_string(normalized.get("team_id") or normalized.get("id"), uppercase=False)
        if not team_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="teams",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="TEAM_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="team_id could not be resolved.",
                )
            )
            continue

        candidate, reject = _build_team_candidate(
            normalized,
            team_id=team_id,
            team_type_domain=team_type_domain,
            team_status_domain=team_status_domain,
            country_domain=country_domain,
            as_of_date=as_of_date,
            context=context,
        )
        if reject is not None:
            rejects.append(reject)
            continue

        candidates.setdefault(team_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="teams",
        target_table=table_name,
        duplicate_rule_id="TEAM_DUPLICATE",
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


def build_club_memberships(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    clubs_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver club_memberships table from Bronze rows."""
    table_name = "club_memberships"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    players_index = _index_rows(players_rows, "player_id")
    clubs_index = _index_rows(clubs_rows, "club_id")
    as_of_date = _resolve_release_as_of_date(monthly_batches_rows)

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        membership_id = standardize_string(
            normalized.get("club_membership_id") or normalized.get("id"),
            uppercase=False,
        )
        if not membership_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="club_memberships",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="CLUB_MEMBERSHIP_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="club_membership_id could not be resolved.",
                )
            )
            continue

        candidate, reject = _build_club_membership_candidate(
            normalized,
            membership_id=membership_id,
            players_index=players_index,
            clubs_index=clubs_index,
            as_of_date=as_of_date,
            context=context,
        )
        if reject is not None:
            rejects.append(reject)
            continue

        candidates.setdefault(membership_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="club_memberships",
        target_table=table_name,
        duplicate_rule_id="CLUB_MEMBERSHIP_DUPLICATE",
    )
    rejects.extend(duplicate_rejects)
    warning_count = _mark_membership_overlaps(
        accepted_rows,
        left_key="player_id",
        right_key="club_id",
        start_key="membership_start_date",
        end_key="membership_end_date",
    )

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


def build_team_memberships(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver team_memberships table from Bronze rows."""
    table_name = "team_memberships"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    players_index = _index_rows(players_rows, "player_id")
    teams_index = _index_rows(teams_rows, "team_id")
    as_of_date = _resolve_release_as_of_date(monthly_batches_rows)
    side_domain = config.data["domains"]["player_position"]

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        membership_id = standardize_string(
            normalized.get("team_membership_id") or normalized.get("id"),
            uppercase=False,
        )
        if not membership_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="team_memberships",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="TEAM_MEMBERSHIP_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="team_membership_id could not be resolved.",
                )
            )
            continue

        candidate, reject = _build_team_membership_candidate(
            normalized,
            membership_id=membership_id,
            players_index=players_index,
            teams_index=teams_index,
            side_domain=side_domain,
            as_of_date=as_of_date,
            context=context,
        )
        if reject is not None:
            rejects.append(reject)
            continue

        candidates.setdefault(membership_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="team_memberships",
        target_table=table_name,
        duplicate_rule_id="TEAM_MEMBERSHIP_DUPLICATE",
    )
    rejects.extend(duplicate_rejects)
    warning_count = _mark_membership_overlaps(
        accepted_rows,
        left_key="player_id",
        right_key="team_id",
        start_key="membership_start_date",
        end_key="membership_end_date",
    )

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


def _build_club_candidate(
    normalized: dict[str, Any],
    *,
    club_id: str,
    region_index: dict[str, dict[str, Any]],
    country_domain: dict[str, Any],
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    club_name = standardize_string(
        normalized.get("club_name") or normalized.get("name"),
        uppercase=False,
    )
    if not club_name:
        return None, build_reject_record(
            context,
            source_table="clubs",
            target_table="clubs",
            source_business_key=club_id,
            reject_reason="MISSING_REQUIRED_COLUMN",
            rule_id="CLUB_002",
            rule_severity="CRITICAL",
            source_record=normalized,
            reject_reason_detail="club_name could not be resolved.",
        )

    region_id = standardize_string(normalized.get("region_id"), uppercase=False)
    if not region_id or region_id not in region_index:
        return None, build_reject_record(
            context,
            source_table="clubs",
            target_table="clubs",
            source_business_key=club_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="CLUB_003",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"region_id '{region_id}' was not found in accepted regions.",
        )

    country_raw = normalized.get("country_code") or normalized.get("country")
    country_code = _normalize_optional_domain_value(country_raw, country_domain)
    if country_raw not in (None, "") and country_code is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="clubs",
            target_table="clubs",
            source_business_key=club_id,
            rule_id="CLUB_004",
            detail=f"Invalid country value '{country_raw}'.",
            source_record=normalized,
        )

    open_date, open_reject = _safe_optional_date(
        normalized.get("open_date") or normalized.get("start_date") or normalized.get("formation_date"),
        context=context,
        source_table="clubs",
        target_table="clubs",
        source_business_key=club_id,
        rule_id="CLUB_005",
        source_record=normalized,
        field_name="open_date",
    )
    if open_reject is not None:
        return None, open_reject

    close_date, close_reject = _safe_optional_date(
        normalized.get("close_date") or normalized.get("end_date") or normalized.get("dissolution_date"),
        context=context,
        source_table="clubs",
        target_table="clubs",
        source_business_key=club_id,
        rule_id="CLUB_006",
        source_record=normalized,
        field_name="close_date",
    )
    if close_reject is not None:
        return None, close_reject

    if open_date is not None and close_date is not None and close_date < open_date:
        return None, build_reject_record(
            context,
            source_table="clubs",
            target_table="clubs",
            source_business_key=club_id,
            reject_reason="INVALID_DATE_RANGE",
            rule_id="CLUB_007",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="close_date cannot be before open_date.",
        )

    region_row = region_index[region_id]
    candidate = {
        "club_id": club_id,
        "club_sk": build_record_hash({"club_id": club_id}, ["club_id"]),
        "club_name": club_name,
        "region_id": region_id,
        "region_sk": region_row["region_sk"],
        "country_code": country_code,
        "open_date": open_date,
        "close_date": close_date,
        "active_flag": _derive_active_flag(normalized),
    }
    candidate.update(
        build_metadata_payload(
            context,
            "clubs",
            build_record_hash(
                {
                    "club_id": club_id,
                    "club_name": club_name,
                    "region_id": region_id,
                    "country_code": country_code,
                },
                ["club_id", "club_name", "region_id", "country_code"],
            ),
        )
    )
    return candidate, None


def _build_team_candidate(
    normalized: dict[str, Any],
    *,
    team_id: str,
    team_type_domain: dict[str, Any],
    team_status_domain: dict[str, Any],
    country_domain: dict[str, Any],
    as_of_date: date | None,
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    team_category_raw = normalized.get("team_category") or normalized.get("category") or normalized.get("team_type")
    team_category = normalize_domain_value(team_category_raw, team_type_domain)
    if team_category_raw not in (None, "") and team_category is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="teams",
            target_table="teams",
            source_business_key=team_id,
            rule_id="TEAM_002",
            detail=f"Invalid team category '{team_category_raw}'.",
            source_record=normalized,
        )

    team_status_raw = normalized.get("team_status") or normalized.get("status")
    team_status = _normalize_optional_domain_value(team_status_raw, team_status_domain)
    if team_status_raw not in (None, "") and team_status is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="teams",
            target_table="teams",
            source_business_key=team_id,
            rule_id="TEAM_003",
            detail=f"Invalid team status '{team_status_raw}'.",
            source_record=normalized,
        )

    country_raw = normalized.get("country_code") or normalized.get("country")
    country_code = _normalize_optional_domain_value(country_raw, country_domain)
    if country_raw not in (None, "") and country_code is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="teams",
            target_table="teams",
            source_business_key=team_id,
            rule_id="TEAM_004",
            detail=f"Invalid country value '{country_raw}'.",
            source_record=normalized,
        )

    formation_date, formation_reject = _safe_optional_date(
        normalized.get("formation_date") or normalized.get("start_date"),
        context=context,
        source_table="teams",
        target_table="teams",
        source_business_key=team_id,
        rule_id="TEAM_005",
        source_record=normalized,
        field_name="formation_date",
    )
    if formation_reject is not None:
        return None, formation_reject

    dissolution_date, dissolution_reject = _safe_optional_date(
        normalized.get("dissolution_date") or normalized.get("end_date"),
        context=context,
        source_table="teams",
        target_table="teams",
        source_business_key=team_id,
        rule_id="TEAM_006",
        source_record=normalized,
        field_name="dissolution_date",
    )
    if dissolution_reject is not None:
        return None, dissolution_reject

    if formation_date is not None and dissolution_date is not None and dissolution_date < formation_date:
        return None, build_reject_record(
            context,
            source_table="teams",
            target_table="teams",
            source_business_key=team_id,
            reject_reason="INVALID_DATE_RANGE",
            rule_id="TEAM_007",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="dissolution_date cannot be before formation_date.",
        )

    active_flag = _derive_active_flag(normalized)
    if active_flag is None and team_status is not None:
        active_flag = team_status == "ACTIVE"

    candidate = {
        "team_id": team_id,
        "team_sk": build_record_hash({"team_id": team_id}, ["team_id"]),
        "team_name": standardize_string(normalized.get("team_name") or normalized.get("name"), uppercase=False),
        "team_category": team_category,
        "country_code": country_code,
        "team_status": team_status,
        "formation_date": formation_date,
        "dissolution_date": dissolution_date,
        "active_flag": active_flag,
        "team_age_days": _duration_days(formation_date, as_of_date) if formation_date and as_of_date else None,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "teams",
            build_record_hash(
                {
                    "team_id": team_id,
                    "team_name": candidate["team_name"],
                    "team_category": team_category,
                    "country_code": country_code,
                },
                ["team_id", "team_name", "team_category", "country_code"],
            ),
        )
    )
    return candidate, None


def _build_club_membership_candidate(
    normalized: dict[str, Any],
    *,
    membership_id: str,
    players_index: dict[str, dict[str, Any]],
    clubs_index: dict[str, dict[str, Any]],
    as_of_date: date | None,
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    player_id = standardize_string(normalized.get("player_id"), uppercase=False)
    if not player_id or player_id not in players_index:
        return None, build_reject_record(
            context,
            source_table="club_memberships",
            target_table="club_memberships",
            source_business_key=membership_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="CLUB_MEMBERSHIP_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"player_id '{player_id}' was not found in accepted players.",
        )

    club_id = standardize_string(normalized.get("club_id"), uppercase=False)
    if not club_id or club_id not in clubs_index:
        return None, build_reject_record(
            context,
            source_table="club_memberships",
            target_table="club_memberships",
            source_business_key=membership_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="CLUB_MEMBERSHIP_003",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"club_id '{club_id}' was not found in accepted clubs.",
        )

    start_date, start_reject = _safe_optional_date(
        normalized.get("membership_start_date") or normalized.get("start_date"),
        context=context,
        source_table="club_memberships",
        target_table="club_memberships",
        source_business_key=membership_id,
        rule_id="CLUB_MEMBERSHIP_004",
        source_record=normalized,
        field_name="membership_start_date",
    )
    if start_reject is not None:
        return None, start_reject

    end_date, end_reject = _safe_optional_date(
        normalized.get("membership_end_date") or normalized.get("end_date"),
        context=context,
        source_table="club_memberships",
        target_table="club_memberships",
        source_business_key=membership_id,
        rule_id="CLUB_MEMBERSHIP_005",
        source_record=normalized,
        field_name="membership_end_date",
    )
    if end_reject is not None:
        return None, end_reject

    if start_date is not None and end_date is not None and end_date < start_date:
        return None, build_reject_record(
            context,
            source_table="club_memberships",
            target_table="club_memberships",
            source_business_key=membership_id,
            reject_reason="INVALID_DATE_RANGE",
            rule_id="CLUB_MEMBERSHIP_006",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="membership_end_date cannot be before membership_start_date.",
        )

    player_row = players_index[player_id]
    club_row = clubs_index[club_id]
    candidate = {
        "club_membership_id": membership_id,
        "club_membership_sk": build_record_hash({"club_membership_id": membership_id}, ["club_membership_id"]),
        "player_id": player_id,
        "player_sk": player_row["player_sk"],
        "club_id": club_id,
        "club_sk": club_row["club_sk"],
        "membership_start_date": start_date,
        "membership_end_date": end_date,
        "membership_duration_days": _duration_days(start_date, end_date),
        "current_membership_flag": _is_current_period(
            effective_start_date=start_date,
            effective_end_date=end_date,
            as_of_date=as_of_date or start_date,
        ),
        "membership_overlap_flag": False,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "club_memberships",
            build_record_hash(
                {
                    "club_membership_id": membership_id,
                    "player_id": player_id,
                    "club_id": club_id,
                    "membership_start_date": start_date,
                    "membership_end_date": end_date,
                },
                [
                    "club_membership_id",
                    "player_id",
                    "club_id",
                    "membership_start_date",
                    "membership_end_date",
                ],
            ),
        )
    )
    return candidate, None


def _build_team_membership_candidate(
    normalized: dict[str, Any],
    *,
    membership_id: str,
    players_index: dict[str, dict[str, Any]],
    teams_index: dict[str, dict[str, Any]],
    side_domain: dict[str, Any],
    as_of_date: date | None,
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    player_id = standardize_string(normalized.get("player_id"), uppercase=False)
    if not player_id or player_id not in players_index:
        return None, build_reject_record(
            context,
            source_table="team_memberships",
            target_table="team_memberships",
            source_business_key=membership_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="TEAM_MEMBERSHIP_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"player_id '{player_id}' was not found in accepted players.",
        )

    team_id = standardize_string(normalized.get("team_id"), uppercase=False)
    if not team_id or team_id not in teams_index:
        return None, build_reject_record(
            context,
            source_table="team_memberships",
            target_table="team_memberships",
            source_business_key=membership_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="TEAM_MEMBERSHIP_003",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"team_id '{team_id}' was not found in accepted teams.",
        )

    start_date, start_reject = _safe_optional_date(
        normalized.get("membership_start_date") or normalized.get("start_date"),
        context=context,
        source_table="team_memberships",
        target_table="team_memberships",
        source_business_key=membership_id,
        rule_id="TEAM_MEMBERSHIP_004",
        source_record=normalized,
        field_name="membership_start_date",
    )
    if start_reject is not None:
        return None, start_reject

    end_date, end_reject = _safe_optional_date(
        normalized.get("membership_end_date") or normalized.get("end_date"),
        context=context,
        source_table="team_memberships",
        target_table="team_memberships",
        source_business_key=membership_id,
        rule_id="TEAM_MEMBERSHIP_005",
        source_record=normalized,
        field_name="membership_end_date",
    )
    if end_reject is not None:
        return None, end_reject

    if start_date is not None and end_date is not None and end_date < start_date:
        return None, build_reject_record(
            context,
            source_table="team_memberships",
            target_table="team_memberships",
            source_business_key=membership_id,
            reject_reason="INVALID_DATE_RANGE",
            rule_id="TEAM_MEMBERSHIP_006",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="membership_end_date cannot be before membership_start_date.",
        )

    position_raw = normalized.get("player_position") or normalized.get("preferred_side") or normalized.get("position")
    player_position = _normalize_optional_domain_value(position_raw, side_domain)
    if position_raw not in (None, "") and player_position is None:
        return None, _invalid_domain_reject(
            context=context,
            source_table="team_memberships",
            target_table="team_memberships",
            source_business_key=membership_id,
            rule_id="TEAM_MEMBERSHIP_007",
            detail=f"Invalid player_position value '{position_raw}'.",
            source_record=normalized,
        )

    player_row = players_index[player_id]
    team_row = teams_index[team_id]
    candidate = {
        "team_membership_id": membership_id,
        "team_membership_sk": build_record_hash({"team_membership_id": membership_id}, ["team_membership_id"]),
        "team_id": team_id,
        "team_sk": team_row["team_sk"],
        "player_id": player_id,
        "player_sk": player_row["player_sk"],
        "membership_start_date": start_date,
        "membership_end_date": end_date,
        "player_role": standardize_string(normalized.get("player_role") or normalized.get("role"), uppercase=True),
        "player_position": player_position,
        "membership_duration_days": _duration_days(start_date, end_date),
        "current_membership_flag": _is_current_period(
            effective_start_date=start_date,
            effective_end_date=end_date,
            as_of_date=as_of_date or start_date,
        ),
        "membership_overlap_flag": False,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "team_memberships",
            build_record_hash(
                {
                    "team_membership_id": membership_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "membership_start_date": start_date,
                    "membership_end_date": end_date,
                },
                [
                    "team_membership_id",
                    "team_id",
                    "player_id",
                    "membership_start_date",
                    "membership_end_date",
                ],
            ),
        )
    )
    return candidate, None


def _mark_membership_overlaps(
    accepted_rows: list[dict[str, Any]],
    *,
    left_key: str,
    right_key: str,
    start_key: str,
    end_key: str,
) -> int:
    overlap_count = 0
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in accepted_rows:
        grouped.setdefault((str(row[left_key]), str(row[right_key])), []).append(row)

    for group_rows in grouped.values():
        ordered = sorted(
            group_rows,
            key=lambda row: (
                row.get(start_key) or date.min,
                row.get(end_key) or date.max,
                str(next(iter(row.values()))),
            ),
        )
        previous_end: date | None = None
        for row in ordered:
            start_date = row.get(start_key)
            if previous_end is not None and start_date is not None and start_date <= previous_end:
                row["membership_overlap_flag"] = True
                overlap_count += 1
            if row.get(end_key) is not None:
                previous_end = row[end_key]
            elif previous_end is None:
                previous_end = date.max
    return overlap_count
