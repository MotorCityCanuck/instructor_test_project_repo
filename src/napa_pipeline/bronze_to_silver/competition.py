"""Competition table builders for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from datetime import date
from typing import Any

from napa_pipeline.bronze_to_silver.athlete import (
    _index_rows,
    _normalize_optional_domain_value,
    _resolve_release_as_of_date,
    _safe_optional_date,
    _safe_optional_float,
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
    _normalize_source_keys,
    _resolve_duplicate_keys,
)
from napa_pipeline.bronze_to_silver.transforms import (
    safe_cast_int,
    standardize_string,
)


COMPLETED_MATCH_STATUSES = {"COMPLETED", "FINAL"}
CANCELLED_MATCH_STATUSES = {"CANCELLED", "FORFEITED"}


def build_matches(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    regions_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    match_teams_source_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver matches table from Bronze rows."""
    table_name = "matches"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    batches_index = _index_rows(monthly_batches_rows, "batch_id")
    regions_index = _index_rows(regions_rows, "region_id")
    winner_team_index = _index_match_team_numbers(match_teams_source_rows)

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        match_id = standardize_string(normalized.get("match_id") or normalized.get("id"), uppercase=False)
        if not match_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="matches",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="MATCH_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="match_id could not be resolved.",
                )
            )
            continue

        candidate, reject = _build_match_candidate(
            normalized,
            match_id=match_id,
            batches_index=batches_index,
            regions_index=regions_index,
            winner_team_index=winner_team_index,
            context=context,
        )
        if reject is not None:
            rejects.append(reject)
            continue

        candidates.setdefault(match_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="matches",
        target_table=table_name,
        duplicate_rule_id="MATCH_DUPLICATE",
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


def build_match_teams(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver match_teams table from Bronze rows."""
    table_name = "match_teams"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    matches_index = _index_rows(matches_rows, "match_id")
    teams_index = _index_rows(teams_rows, "team_id")

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        match_team_id = standardize_string(normalized.get("match_team_id") or normalized.get("id"), uppercase=False)
        if not match_team_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="match_teams",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="MATCH_TEAM_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="match_team_id could not be resolved.",
                )
            )
            continue

        candidate, reject = _build_match_team_candidate(
            normalized,
            match_team_id=match_team_id,
            matches_index=matches_index,
            teams_index=teams_index,
            context=context,
        )
        if reject is not None:
            rejects.append(reject)
            continue

        candidates.setdefault(match_team_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="match_teams",
        target_table=table_name,
        duplicate_rule_id="MATCH_TEAM_DUPLICATE",
    )
    rejects.extend(duplicate_rejects)

    warning_count = _mark_match_side_warnings(
        accepted_rows,
        expected_match_team_count=int(config.data["thresholds"]["expected_match_team_count"]),
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


def build_match_team_players(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> SilverBuildResult:
    """Build the Silver match_team_players table from Bronze rows."""
    table_name = "match_team_players"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    match_teams_index = _index_rows(match_teams_rows, "match_team_id")
    players_index = _index_rows(players_rows, "player_id")
    side_domain = config.data["domains"]["player_position"]

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        participant_id = standardize_string(
            normalized.get("match_team_player_id") or normalized.get("id"),
            uppercase=False,
        )
        if not participant_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="match_team_players",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="MATCH_TEAM_PLAYER_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="match_team_player_id could not be resolved.",
                )
            )
            continue

        candidate, reject = _build_match_team_player_candidate(
            normalized,
            participant_id=participant_id,
            match_teams_index=match_teams_index,
            players_index=players_index,
            side_domain=side_domain,
            context=context,
        )
        if reject is not None:
            rejects.append(reject)
            continue

        candidates.setdefault(participant_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="match_team_players",
        target_table=table_name,
        duplicate_rule_id="MATCH_TEAM_PLAYER_DUPLICATE",
    )
    rejects.extend(duplicate_rejects)

    accepted_rows, structural_rejects, warning_count = _enforce_match_team_player_structure(
        accepted_rows,
        context=context,
        expected_player_count=int(config.data["thresholds"]["expected_match_team_player_count"]),
        team_memberships_rows=team_memberships_rows,
    )
    rejects.extend(structural_rejects)

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


def build_match_games(
    source_rows: list[dict[str, Any]],
    config: BronzeToSilverConfig,
    context: PipelineContext,
    *,
    matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> SilverBuildResult:
    """Build the Silver match_games table from Bronze rows."""
    table_name = "match_games"
    exact_deduped, exact_duplicate_count = _dedupe_exact_rows(source_rows)
    matches_index = _index_rows(matches_rows, "match_id")

    candidates: dict[str, list[dict[str, Any]]] = {}
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    for source_row in exact_deduped:
        normalized = _normalize_source_keys(source_row)
        match_game_id = standardize_string(normalized.get("match_game_id") or normalized.get("id"), uppercase=False)
        if not match_game_id:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="match_games",
                    target_table=table_name,
                    source_business_key="<NULL>",
                    reject_reason="MISSING_PRIMARY_KEY",
                    rule_id="MATCH_GAME_001",
                    rule_severity="CRITICAL",
                    source_record=normalized,
                    reject_reason_detail="match_game_id could not be resolved.",
                )
            )
            continue

        candidate, reject = _build_match_game_candidate(
            normalized,
            match_game_id=match_game_id,
            matches_index=matches_index,
            context=context,
            close_game_margin=int(config.data["thresholds"]["close_game_margin"]),
            score_share_tolerance=float(config.data["thresholds"]["score_share_tolerance"]),
        )
        if reject is not None:
            rejects.append(reject)
            continue

        candidates.setdefault(match_game_id, []).append(candidate)

    accepted_rows, duplicate_rejects, business_key_duplicate_count = _resolve_duplicate_keys(
        candidates,
        context=context,
        source_table="match_games",
        target_table=table_name,
        duplicate_rule_id="MATCH_GAME_DUPLICATE",
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


def _build_match_candidate(
    normalized: dict[str, Any],
    *,
    match_id: str,
    batches_index: dict[str, dict[str, Any]],
    regions_index: dict[str, dict[str, Any]],
    winner_team_index: dict[tuple[str, str], set[int]],
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    batch_id = standardize_string(normalized.get("batch_id"), uppercase=False)
    if batch_id and batch_id not in batches_index:
        return None, build_reject_record(
            context,
            source_table="matches",
            target_table="matches",
            source_business_key=match_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="MATCH_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"batch_id '{batch_id}' was not found in accepted monthly_batches.",
        )

    region_id = standardize_string(normalized.get("region_id"), uppercase=False)
    if region_id and region_id not in regions_index:
        return None, build_reject_record(
            context,
            source_table="matches",
            target_table="matches",
            source_business_key=match_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="MATCH_003",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"region_id '{region_id}' was not found in accepted regions.",
        )

    match_date, date_reject = _safe_optional_date(
        normalized.get("match_date") or normalized.get("date"),
        context=context,
        source_table="matches",
        target_table="matches",
        source_business_key=match_id,
        rule_id="MATCH_004",
        source_record=normalized,
        field_name="match_date",
    )
    if date_reject is not None:
        return None, date_reject

    status = standardize_string(normalized.get("match_status") or normalized.get("status"), uppercase=True)
    explicit_winner, winner_reject = _safe_optional_side_number(
        normalized.get("winning_team_number") or normalized.get("winner_team_number"),
        context=context,
        source_table="matches",
        target_table="matches",
        source_business_key=match_id,
        rule_id="MATCH_005",
        source_record=normalized,
        field_name="winning_team_number",
    )
    if winner_reject is not None:
        return None, winner_reject
    winner_team_id = standardize_string(
        normalized.get("winning_team_id") or normalized.get("winner_team_id"),
        uppercase=False,
    )
    derived_winner = _resolve_winning_team_number(
        match_id,
        winner_team_id,
        winner_team_index=winner_team_index,
    )
    if explicit_winner is not None and derived_winner is not None and explicit_winner != derived_winner:
        return None, build_reject_record(
            context,
            source_table="matches",
            target_table="matches",
            source_business_key=match_id,
            reject_reason="VALUE_OUT_OF_RANGE",
            rule_id="MATCH_005",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=(
                "winning_team_number is inconsistent with the winning_team_id mapping."
            ),
        )
    winner = explicit_winner if explicit_winner is not None else derived_winner
    if winner_team_id and winner is None:
        return None, build_reject_record(
            context,
            source_table="matches",
            target_table="matches",
            source_business_key=match_id,
            reject_reason="VALUE_OUT_OF_RANGE",
            rule_id="MATCH_005",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=(
                f"winning_team_id '{winner_team_id}' could not be resolved to team_number 1 or 2 "
                "from the match_teams source."
            ),
        )

    completed_flag = status in COMPLETED_MATCH_STATUSES or winner is not None
    if completed_flag and winner is None:
        return None, build_reject_record(
            context,
            source_table="matches",
            target_table="matches",
            source_business_key=match_id,
            reject_reason="VALUE_OUT_OF_RANGE",
            rule_id="MATCH_006",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="Completed matches require a winning_team_number.",
        )
    if status in CANCELLED_MATCH_STATUSES and completed_flag:
        return None, build_reject_record(
            context,
            source_table="matches",
            target_table="matches",
            source_business_key=match_id,
            reject_reason="VALUE_OUT_OF_RANGE",
            rule_id="MATCH_007",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="Cancelled or forfeited matches cannot be completed.",
        )

    batch_row = batches_index.get(batch_id) if batch_id else None
    region_row = regions_index.get(region_id) if region_id else None
    candidate = {
        "match_id": match_id,
        "match_sk": build_record_hash({"match_id": match_id}, ["match_id"]),
        "batch_id": batch_id,
        "batch_sk": batch_row.get("batch_sk") if batch_row else None,
        "region_id": region_id,
        "region_sk": region_row.get("region_sk") if region_row else None,
        "match_date": match_date,
        "match_type": standardize_string(normalized.get("match_type"), uppercase=True),
        "competition_category": standardize_string(
            normalized.get("competition_category") or normalized.get("category"),
            uppercase=True,
        ),
        "match_status": status,
        "winning_team_number": winner,
        "completed_flag": completed_flag,
        "match_year": match_date.year if match_date else None,
        "match_month": match_date.month if match_date else None,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "matches",
            build_record_hash(
                {
                    "match_id": match_id,
                    "batch_id": batch_id,
                    "region_id": region_id,
                    "match_date": match_date,
                    "winning_team_number": winner,
                },
                ["match_id", "batch_id", "region_id", "match_date", "winning_team_number"],
            ),
        )
    )
    return candidate, None


def _index_match_team_numbers(
    match_teams_source_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[tuple[str, str], set[int]]:
    index: dict[tuple[str, str], set[int]] = {}
    for source_row in match_teams_source_rows:
        normalized = _normalize_source_keys(source_row)
        match_id = standardize_string(normalized.get("match_id"), uppercase=False)
        team_id = standardize_string(normalized.get("team_id"), uppercase=False)
        if not match_id or not team_id:
            continue
        try:
            team_number = safe_cast_int(
                normalized.get("team_number") or normalized.get("side_number")
            )
        except Exception:
            team_number = None
        if team_number not in (1, 2):
            continue
        index.setdefault((match_id, team_id), set()).add(team_number)
    return index


def _resolve_winning_team_number(
    match_id: str,
    winner_team_id: str | None,
    *,
    winner_team_index: dict[tuple[str, str], set[int]],
) -> int | None:
    if winner_team_id is None:
        return None
    candidate_numbers = winner_team_index.get((match_id, winner_team_id), set())
    if len(candidate_numbers) != 1:
        return None
    return next(iter(candidate_numbers))


def _build_match_team_candidate(
    normalized: dict[str, Any],
    *,
    match_team_id: str,
    matches_index: dict[str, dict[str, Any]],
    teams_index: dict[str, dict[str, Any]],
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    match_id = standardize_string(normalized.get("match_id"), uppercase=False)
    if not match_id or match_id not in matches_index:
        return None, build_reject_record(
            context,
            source_table="match_teams",
            target_table="match_teams",
            source_business_key=match_team_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="MATCH_TEAM_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"match_id '{match_id}' was not found in accepted matches.",
        )

    team_id = standardize_string(normalized.get("team_id"), uppercase=False)
    team_row = teams_index.get(team_id) if team_id else None

    team_number, team_number_reject = _safe_optional_side_number(
        normalized.get("team_number") or normalized.get("side_number"),
        context=context,
        source_table="match_teams",
        target_table="match_teams",
        source_business_key=match_team_id,
        rule_id="MATCH_TEAM_003",
        source_record=normalized,
        field_name="team_number",
        required=True,
    )
    if team_number_reject is not None:
        return None, team_number_reject

    pre_rating, pre_rating_reject = _safe_optional_float(
        normalized.get("pre_match_team_rating") or normalized.get("team_rating_before"),
        context=context,
        source_table="match_teams",
        target_table="match_teams",
        source_business_key=match_team_id,
        rule_id="MATCH_TEAM_004",
        source_record=normalized,
        field_name="pre_match_team_rating",
    )
    if pre_rating_reject is not None:
        return None, pre_rating_reject
    post_rating, post_rating_reject = _safe_optional_float(
        normalized.get("post_match_team_rating") or normalized.get("team_rating_after"),
        context=context,
        source_table="match_teams",
        target_table="match_teams",
        source_business_key=match_team_id,
        rule_id="MATCH_TEAM_005",
        source_record=normalized,
        field_name="post_match_team_rating",
    )
    if post_rating_reject is not None:
        return None, post_rating_reject

    match_row = matches_index[match_id]
    winner_flag = (
        match_row.get("winning_team_number") == team_number
        if match_row.get("winning_team_number") is not None
        else None
    )
    candidate = {
        "match_team_id": match_team_id,
        "match_team_sk": build_record_hash({"match_team_id": match_team_id}, ["match_team_id"]),
        "match_id": match_id,
        "match_sk": match_row["match_sk"],
        "match_date": match_row.get("match_date"),
        "team_id": team_id,
        "team_sk": team_row.get("team_sk") if team_row else None,
        "team_number": team_number,
        "winner_flag": winner_flag,
        "pre_match_team_rating": pre_rating,
        "post_match_team_rating": post_rating,
        "rating_change": (post_rating - pre_rating) if pre_rating is not None and post_rating is not None else None,
        "side_cardinality_warning_flag": False,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "match_teams",
            build_record_hash(
                {
                    "match_team_id": match_team_id,
                    "match_id": match_id,
                    "team_id": team_id,
                    "team_number": team_number,
                },
                ["match_team_id", "match_id", "team_id", "team_number"],
            ),
        )
    )
    return candidate, None


def _build_match_team_player_candidate(
    normalized: dict[str, Any],
    *,
    participant_id: str,
    match_teams_index: dict[str, dict[str, Any]],
    players_index: dict[str, dict[str, Any]],
    side_domain: dict[str, Any],
    context: PipelineContext,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    match_team_id = standardize_string(normalized.get("match_team_id"), uppercase=False)
    if not match_team_id or match_team_id not in match_teams_index:
        return None, build_reject_record(
            context,
            source_table="match_team_players",
            target_table="match_team_players",
            source_business_key=participant_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="MATCH_TEAM_PLAYER_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"match_team_id '{match_team_id}' was not found in accepted match_teams.",
        )

    player_id = standardize_string(normalized.get("player_id"), uppercase=False)
    if not player_id or player_id not in players_index:
        return None, build_reject_record(
            context,
            source_table="match_team_players",
            target_table="match_team_players",
            source_business_key=participant_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="MATCH_TEAM_PLAYER_003",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"player_id '{player_id}' was not found in accepted players.",
        )

    player_position_raw = normalized.get("player_position")
    position_alias_raw = normalized.get("position")
    player_position = _normalize_optional_domain_value(
        player_position_raw or position_alias_raw,
        side_domain,
    )
    if player_position_raw not in (None, "") and player_position is None:
        return None, build_reject_record(
            context,
            source_table="match_team_players",
            target_table="match_team_players",
            source_business_key=participant_id,
            reject_reason="INVALID_DOMAIN_VALUE",
            rule_id="MATCH_TEAM_PLAYER_004",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"Invalid player_position value '{player_position_raw}'.",
        )

    rating, rating_reject = _safe_optional_float(
        normalized.get("player_rating_at_match") or normalized.get("rating_at_match"),
        context=context,
        source_table="match_team_players",
        target_table="match_team_players",
        source_business_key=participant_id,
        rule_id="MATCH_TEAM_PLAYER_005",
        source_record=normalized,
        field_name="player_rating_at_match",
    )
    if rating_reject is not None:
        return None, rating_reject

    match_team_row = match_teams_index[match_team_id]
    player_row = players_index[player_id]
    candidate = {
        "match_team_player_id": participant_id,
        "match_team_player_sk": build_record_hash({"match_team_player_id": participant_id}, ["match_team_player_id"]),
        "match_team_id": match_team_id,
        "match_team_sk": match_team_row["match_team_sk"],
        "match_id": match_team_row["match_id"],
        "match_sk": match_team_row["match_sk"],
        "match_date": match_team_row.get("match_date"),
        "team_id": match_team_row["team_id"],
        "team_sk": match_team_row["team_sk"],
        "player_id": player_id,
        "player_sk": player_row["player_sk"],
        "player_position": player_position,
        "player_rating_at_match": rating,
        "membership_history_warning_flag": False,
    }
    candidate.update(
        build_metadata_payload(
            context,
            "match_team_players",
            build_record_hash(
                {
                    "match_team_player_id": participant_id,
                    "match_team_id": match_team_id,
                    "player_id": player_id,
                },
                ["match_team_player_id", "match_team_id", "player_id"],
            ),
        )
    )
    return candidate, None


def _build_match_game_candidate(
    normalized: dict[str, Any],
    *,
    match_game_id: str,
    matches_index: dict[str, dict[str, Any]],
    context: PipelineContext,
    close_game_margin: int,
    score_share_tolerance: float,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    match_id = standardize_string(normalized.get("match_id"), uppercase=False)
    if not match_id or match_id not in matches_index:
        return None, build_reject_record(
            context,
            source_table="match_games",
            target_table="match_games",
            source_business_key=match_game_id,
            reject_reason="ORPHAN_FOREIGN_KEY",
            rule_id="MATCH_GAME_002",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail=f"match_id '{match_id}' was not found in accepted matches.",
        )

    game_number, game_number_reject = _safe_positive_int(
        normalized.get("game_number"),
        context=context,
        source_table="match_games",
        target_table="match_games",
        source_business_key=match_game_id,
        rule_id="MATCH_GAME_003",
        source_record=normalized,
        field_name="game_number",
    )
    if game_number_reject is not None:
        return None, game_number_reject

    team_one_score, score1_reject = _safe_nonnegative_int(
        normalized.get("team_one_score"),
        context=context,
        source_table="match_games",
        target_table="match_games",
        source_business_key=match_game_id,
        rule_id="MATCH_GAME_004",
        source_record=normalized,
        field_name="team_one_score",
    )
    if score1_reject is not None:
        return None, score1_reject
    team_two_score, score2_reject = _safe_nonnegative_int(
        normalized.get("team_two_score"),
        context=context,
        source_table="match_games",
        target_table="match_games",
        source_business_key=match_game_id,
        rule_id="MATCH_GAME_005",
        source_record=normalized,
        field_name="team_two_score",
    )
    if score2_reject is not None:
        return None, score2_reject

    winner, winner_reject = _safe_optional_side_number(
        normalized.get("winning_team_number") or normalized.get("winner_team_number"),
        context=context,
        source_table="match_games",
        target_table="match_games",
        source_business_key=match_game_id,
        rule_id="MATCH_GAME_006",
        source_record=normalized,
        field_name="winning_team_number",
        required=True,
    )
    if winner_reject is not None:
        return None, winner_reject

    derived_winner = 1 if team_one_score > team_two_score else 2 if team_two_score > team_one_score else None
    if derived_winner is None or winner != derived_winner:
        return None, build_reject_record(
            context,
            source_table="match_games",
            target_table="match_games",
            source_business_key=match_game_id,
            reject_reason="GAME_SCORE_WINNER_MISMATCH",
            rule_id="MATCH_GAME_007",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="winning_team_number is inconsistent with the game scores.",
        )

    target_score, target_score_reject = _safe_nonnegative_int(
        normalized.get("target_score"),
        context=context,
        source_table="match_games",
        target_table="match_games",
        source_business_key=match_game_id,
        rule_id="MATCH_GAME_008",
        source_record=normalized,
        field_name="target_score",
        required=False,
    )
    if target_score_reject is not None:
        return None, target_score_reject

    win_by, win_by_reject = _safe_nonnegative_int(
        normalized.get("win_by"),
        context=context,
        source_table="match_games",
        target_table="match_games",
        source_business_key=match_game_id,
        rule_id="MATCH_GAME_009",
        source_record=normalized,
        field_name="win_by",
        required=False,
    )
    if win_by_reject is not None:
        return None, win_by_reject

    actual_share, share_reject = _safe_optional_float(
        normalized.get("actual_team_one_score_share"),
        context=context,
        source_table="match_games",
        target_table="match_games",
        source_business_key=match_game_id,
        rule_id="MATCH_GAME_010",
        source_record=normalized,
        field_name="actual_team_one_score_share",
    )
    if share_reject is not None:
        return None, share_reject

    total_points = team_one_score + team_two_score
    derived_share = (team_one_score / total_points) if total_points else None
    if actual_share is not None and derived_share is not None and abs(actual_share - derived_share) > score_share_tolerance:
        return None, build_reject_record(
            context,
            source_table="match_games",
            target_table="match_games",
            source_business_key=match_game_id,
            reject_reason="VALUE_OUT_OF_RANGE",
            rule_id="MATCH_GAME_011",
            rule_severity="ERROR",
            source_record=normalized,
            reject_reason_detail="actual_team_one_score_share does not reconcile with the scores.",
        )

    match_row = matches_index[match_id]
    score_margin = abs(team_one_score - team_two_score)
    winning_score = max(team_one_score, team_two_score)
    candidate = {
        "match_game_id": match_game_id,
        "match_game_sk": build_record_hash({"match_game_id": match_game_id}, ["match_game_id"]),
        "match_id": match_id,
        "match_sk": match_row["match_sk"],
        "game_number": game_number,
        "team_one_score": team_one_score,
        "team_two_score": team_two_score,
        "winning_team_number": winner,
        "target_score": target_score,
        "win_by": win_by,
        "actual_team_one_score_share": actual_share,
        "score_margin": score_margin,
        "total_points": total_points,
        "close_game_flag": score_margin <= close_game_margin,
        "extended_game_flag": bool(target_score is not None and winning_score > target_score),
    }
    candidate.update(
        build_metadata_payload(
            context,
            "match_games",
            build_record_hash(
                {
                    "match_game_id": match_game_id,
                    "match_id": match_id,
                    "game_number": game_number,
                    "team_one_score": team_one_score,
                    "team_two_score": team_two_score,
                },
                ["match_game_id", "match_id", "game_number", "team_one_score", "team_two_score"],
            ),
        )
    )
    return candidate, None


def _safe_optional_side_number(
    value: Any,
    *,
    context: PipelineContext,
    source_table: str,
    target_table: str,
    source_business_key: str,
    rule_id: str,
    source_record: dict[str, Any],
    field_name: str,
    required: bool = False,
) -> tuple[int | None, dict[str, Any] | None]:
    if value in (None, ""):
        if required:
            return None, build_reject_record(
                context,
                source_table=source_table,
                target_table=target_table,
                source_business_key=source_business_key,
                reject_reason="MISSING_REQUIRED_COLUMN",
                rule_id=rule_id,
                rule_severity="ERROR",
                source_record=source_record,
                reject_reason_detail=f"{field_name} is required.",
            )
        return None, None
    try:
        side_number = safe_cast_int(value)
    except Exception:
        side_number = None
    if side_number not in (1, 2):
        return None, build_reject_record(
            context,
            source_table=source_table,
            target_table=target_table,
            source_business_key=source_business_key,
            reject_reason="VALUE_OUT_OF_RANGE",
            rule_id=rule_id,
            rule_severity="ERROR",
            source_record=source_record,
            reject_reason_detail=f"{field_name} must be 1 or 2.",
        )
    return side_number, None


def _safe_positive_int(
    value: Any,
    **kwargs: Any,
) -> tuple[int | None, dict[str, Any] | None]:
    result, reject = _safe_nonnegative_int(value, **kwargs)
    if reject is not None or result is None:
        return result, reject
    if result <= 0:
        return None, build_reject_record(
            kwargs["context"],
            source_table=kwargs["source_table"],
            target_table=kwargs["target_table"],
            source_business_key=kwargs["source_business_key"],
            reject_reason="VALUE_OUT_OF_RANGE",
            rule_id=kwargs["rule_id"],
            rule_severity="ERROR",
            source_record=kwargs["source_record"],
            reject_reason_detail=f"{kwargs['field_name']} must be positive.",
        )
    return result, None


def _safe_nonnegative_int(
    value: Any,
    *,
    context: PipelineContext,
    source_table: str,
    target_table: str,
    source_business_key: str,
    rule_id: str,
    source_record: dict[str, Any],
    field_name: str,
    required: bool = True,
) -> tuple[int | None, dict[str, Any] | None]:
    if value in (None, ""):
        if required:
            return None, build_reject_record(
                context,
                source_table=source_table,
                target_table=target_table,
                source_business_key=source_business_key,
                reject_reason="MISSING_REQUIRED_COLUMN",
                rule_id=rule_id,
                rule_severity="ERROR",
                source_record=source_record,
                reject_reason_detail=f"{field_name} is required.",
            )
        return None, None
    try:
        integer_value = safe_cast_int(value)
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
    if integer_value < 0:
        return None, build_reject_record(
            context,
            source_table=source_table,
            target_table=target_table,
            source_business_key=source_business_key,
            reject_reason="VALUE_OUT_OF_RANGE",
            rule_id=rule_id,
            rule_severity="ERROR",
            source_record=source_record,
            reject_reason_detail=f"{field_name} must be non-negative.",
        )
    return integer_value, None


def _mark_match_side_warnings(
    accepted_rows: list[dict[str, Any]],
    *,
    expected_match_team_count: int,
) -> int:
    warning_count = 0
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in accepted_rows:
        grouped.setdefault(str(row["match_id"]), []).append(row)
    for rows in grouped.values():
        if len(rows) != expected_match_team_count:
            warning_count += 1
            for row in rows:
                row["side_cardinality_warning_flag"] = True
    return warning_count


def _enforce_match_team_player_structure(
    accepted_rows: list[dict[str, Any]],
    *,
    context: PipelineContext,
    expected_player_count: int,
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    membership_index = _index_memberships(team_memberships_rows)
    retained: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    warning_count = 0

    seen_by_match_side: set[tuple[str, str]] = set()
    seen_by_match_player: dict[tuple[str, str], str] = {}
    for row in accepted_rows:
        side_key = (str(row["match_team_id"]), str(row["player_id"]))
        if side_key in seen_by_match_side:
            rejects.append(
                build_reject_record(
                    context,
                    source_table="match_team_players",
                    target_table="match_team_players",
                    source_business_key=str(row["match_team_player_id"]),
                    reject_reason="INVALID_PARTICIPANT_CARDINALITY",
                    rule_id="MATCH_TEAM_PLAYER_006",
                    rule_severity="ERROR",
                    source_record=row,
                    reject_reason_detail="player appears more than once on the same match side.",
                )
            )
            continue
        match_player_key = (str(row["match_id"]), str(row["player_id"]))
        previous_side = seen_by_match_player.get(match_player_key)
        if previous_side is not None and previous_side != str(row["match_team_id"]):
            rejects.append(
                build_reject_record(
                    context,
                    source_table="match_team_players",
                    target_table="match_team_players",
                    source_business_key=str(row["match_team_player_id"]),
                    reject_reason="PLAYER_ON_BOTH_MATCH_SIDES",
                    rule_id="MATCH_TEAM_PLAYER_007",
                    rule_severity="ERROR",
                    source_record=row,
                    reject_reason_detail="player appears on both sides of the same match.",
                )
            )
            continue

        match_date = row.get("match_date")
        membership_key = (str(row["team_id"]), str(row["player_id"]))
        memberships = membership_index.get(membership_key, [])
        if memberships and isinstance(match_date, date):
            valid_membership = any(_membership_covers_date(membership, match_date) for membership in memberships)
            if not valid_membership:
                row["membership_history_warning_flag"] = True
                warning_count += 1

        seen_by_match_side.add(side_key)
        seen_by_match_player[match_player_key] = str(row["match_team_id"])
        retained.append(row)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in retained:
        grouped.setdefault(str(row["match_team_id"]), []).append(row)
    for rows in grouped.values():
        if len(rows) != expected_player_count:
            warning_count += 1
    return retained, rejects, warning_count


def _index_memberships(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in team_memberships_rows:
        team_id = row.get("team_id")
        player_id = row.get("player_id")
        if team_id in (None, "") or player_id in (None, ""):
            continue
        grouped.setdefault((str(team_id), str(player_id)), []).append(row)
    return grouped


def _membership_covers_date(membership: dict[str, Any], match_date: date) -> bool:
    start_date = membership.get("membership_start_date")
    end_date = membership.get("membership_end_date")
    if isinstance(start_date, date) and start_date > match_date:
        return False
    if isinstance(end_date, date) and end_date < match_date:
        return False
    return True
