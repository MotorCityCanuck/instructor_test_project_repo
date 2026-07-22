"""Persistent-team resolution helpers for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.publish import publish_stage_to_gold_table
from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment


DIRECT_VALID_TEAM_ID = "DIRECT_VALID_TEAM_ID"
ACTIVE_MEMBERSHIP_PAIR = "ACTIVE_MEMBERSHIP_PAIR"
UNIQUE_HISTORICAL_PAIR = "UNIQUE_HISTORICAL_PAIR"
UNRESOLVED = "UNRESOLVED"
AMBIGUOUS = "AMBIGUOUS"
RESOLVED = "RESOLVED"

DIRECT_RESOLUTION_CONFIDENCE = 1.0
ACTIVE_PAIR_RESOLUTION_CONFIDENCE = 0.9
HISTORICAL_PAIR_RESOLUTION_CONFIDENCE = 0.6
UNRESOLVED_CONFIDENCE = 0.0


@dataclass(frozen=True)
class ResolvedMatchTeamsResult:
    """Resolved match-side rows and summary counts for one Phase 4 build."""

    rows: tuple[dict[str, Any], ...]
    direct_resolution_count: int
    active_pair_resolution_count: int
    historical_pair_resolution_count: int
    ambiguous_count: int
    unresolved_count: int
    persistent_team_resolution_pct: float


@dataclass(frozen=True)
class ResolvedMatchTeamsPublicationSummary:
    """Published-table summary for the Spark-native Phase 4 build."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int
    direct_resolution_count: int
    active_pair_resolution_count: int
    historical_pair_resolution_count: int
    ambiguous_count: int
    unresolved_count: int
    persistent_team_resolution_pct: float


def build_resolved_match_teams(
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> ResolvedMatchTeamsResult:
    """Resolve persistent teams for historical match sides without fabricating team IDs."""
    teams_by_id = {
        str(row["team_id"]): row
        for row in teams_rows
        if row.get("team_id") not in (None, "")
    }
    player_ids_by_match_team = _group_player_ids_by_match_team(match_team_players_rows)
    active_pair_index = _build_active_pair_index(team_memberships_rows, teams_by_id)
    historical_pair_index = _build_historical_pair_index(team_memberships_rows)

    resolved_rows: list[dict[str, Any]] = []
    direct_resolution_count = 0
    active_pair_resolution_count = 0
    historical_pair_resolution_count = 0
    ambiguous_count = 0
    unresolved_count = 0

    for match_team_row in match_teams_rows:
        resolved_row = _resolve_match_team_row(
            match_team_row,
            teams_by_id=teams_by_id,
            player_ids_by_match_team=player_ids_by_match_team,
            active_pair_index=active_pair_index,
            historical_pair_index=historical_pair_index,
        )
        resolved_rows.append(resolved_row)

        method = resolved_row["team_resolution_method"]
        status = resolved_row["team_resolution_status"]
        if method == DIRECT_VALID_TEAM_ID:
            direct_resolution_count += 1
        elif method == ACTIVE_MEMBERSHIP_PAIR:
            active_pair_resolution_count += 1
        elif method == UNIQUE_HISTORICAL_PAIR:
            historical_pair_resolution_count += 1
        elif status == AMBIGUOUS:
            ambiguous_count += 1
        else:
            unresolved_count += 1

    resolved_count = (
        direct_resolution_count
        + active_pair_resolution_count
        + historical_pair_resolution_count
    )
    persistent_team_resolution_pct = (
        (resolved_count / len(resolved_rows)) * 100.0 if resolved_rows else 0.0
    )

    return ResolvedMatchTeamsResult(
        rows=tuple(resolved_rows),
        direct_resolution_count=direct_resolution_count,
        active_pair_resolution_count=active_pair_resolution_count,
        historical_pair_resolution_count=historical_pair_resolution_count,
        ambiguous_count=ambiguous_count,
        unresolved_count=unresolved_count,
        persistent_team_resolution_pct=persistent_team_resolution_pct,
    )


def build_resolved_match_teams_sql(environment: ReleaseEnvironment) -> str:
    """Return the Spark SQL used to build the Gold resolved_match_teams table."""
    matches_fqn = get_silver_source_table_fqn(environment, "matches")
    match_teams_fqn = get_silver_source_table_fqn(environment, "match_teams")
    match_team_players_fqn = get_silver_source_table_fqn(environment, "match_team_players")
    team_memberships_fqn = get_silver_source_table_fqn(environment, "team_memberships")
    teams_fqn = get_silver_source_table_fqn(environment, "teams")

    return f"""
WITH match_team_base AS (
    SELECT
        CAST(mt.match_id AS STRING) AS match_id,
        CAST(mt.match_team_id AS STRING) AS match_team_id,
        mt.team_number AS team_number,
        CAST(mt.team_id AS STRING) AS direct_team_id,
        COALESCE(CAST(mt.match_date AS DATE), CAST(m.match_date AS DATE)) AS match_date
    FROM {match_teams_fqn} AS mt
    LEFT JOIN {matches_fqn} AS m
      ON CAST(mt.match_id AS STRING) = CAST(m.match_id AS STRING)
),
match_team_pairs AS (
    SELECT
        CAST(match_team_id AS STRING) AS match_team_id,
        array_sort(collect_set(CAST(player_id AS STRING))) AS player_ids
    FROM {match_team_players_fqn}
    WHERE player_id IS NOT NULL
      AND match_team_id IS NOT NULL
    GROUP BY CAST(match_team_id AS STRING)
),
base AS (
    SELECT
        mtb.match_id,
        mtb.match_team_id,
        mtb.team_number,
        mtb.direct_team_id,
        mtb.match_date,
        CASE WHEN size(mtp.player_ids) = 2 THEN element_at(mtp.player_ids, 1) END AS player_one_id,
        CASE WHEN size(mtp.player_ids) = 2 THEN element_at(mtp.player_ids, 2) END AS player_two_id,
        CASE
            WHEN size(mtp.player_ids) = 2 THEN concat_ws(':', element_at(mtp.player_ids, 1), element_at(mtp.player_ids, 2))
        END AS canonical_player_pair_key
    FROM match_team_base AS mtb
    LEFT JOIN match_team_pairs AS mtp
      ON mtb.match_team_id = mtp.match_team_id
),
teams_normalized AS (
    SELECT
        CAST(team_id AS STRING) AS team_id,
        CAST(formation_date AS DATE) AS formation_date,
        CAST(dissolution_date AS DATE) AS dissolution_date,
        active_flag,
        UPPER(TRIM(CAST(team_status AS STRING))) AS team_status
    FROM {teams_fqn}
    WHERE team_id IS NOT NULL
),
team_memberships_deduped AS (
    SELECT DISTINCT
        CAST(team_id AS STRING) AS team_id,
        CAST(player_id AS STRING) AS player_id,
        CAST(membership_start_date AS DATE) AS membership_start_date,
        CAST(membership_end_date AS DATE) AS membership_end_date
    FROM {team_memberships_fqn}
    WHERE team_id IS NOT NULL
      AND player_id IS NOT NULL
),
membership_pairs AS (
    SELECT
        m1.team_id AS team_id,
        m1.player_id AS player_one_id,
        m2.player_id AS player_two_id,
        greatest(m1.membership_start_date, m2.membership_start_date) AS overlap_start_date,
        least(m1.membership_end_date, m2.membership_end_date) AS overlap_end_date
    FROM team_memberships_deduped AS m1
    INNER JOIN team_memberships_deduped AS m2
      ON m1.team_id = m2.team_id
     AND m1.player_id < m2.player_id
    WHERE
        greatest(m1.membership_start_date, m2.membership_start_date) IS NULL
        OR least(m1.membership_end_date, m2.membership_end_date) IS NULL
        OR greatest(m1.membership_start_date, m2.membership_start_date)
           <= least(m1.membership_end_date, m2.membership_end_date)
),
active_pair_candidates AS (
    SELECT
        b.match_team_id,
        sort_array(collect_set(mp.team_id)) AS active_team_ids,
        COUNT(DISTINCT mp.team_id) AS active_team_count
    FROM base AS b
    INNER JOIN membership_pairs AS mp
      ON b.player_one_id = mp.player_one_id
     AND b.player_two_id = mp.player_two_id
    INNER JOIN teams_normalized AS t
      ON mp.team_id = t.team_id
    WHERE b.match_date IS NOT NULL
      AND (mp.overlap_start_date IS NULL OR b.match_date >= mp.overlap_start_date)
      AND (mp.overlap_end_date IS NULL OR b.match_date <= mp.overlap_end_date)
      AND (t.formation_date IS NULL OR b.match_date >= t.formation_date)
      AND (t.dissolution_date IS NULL OR b.match_date <= t.dissolution_date)
    GROUP BY b.match_team_id
),
historical_pair_candidates AS (
    SELECT
        player_one_id,
        player_two_id,
        sort_array(collect_set(team_id)) AS historical_team_ids,
        COUNT(DISTINCT team_id) AS historical_team_count
    FROM membership_pairs
    GROUP BY player_one_id, player_two_id
),
resolved_pre AS (
    SELECT
        b.match_id,
        b.match_team_id,
        b.team_number,
        b.match_date,
        b.player_one_id,
        b.player_two_id,
        b.canonical_player_pair_key,
        CASE
            WHEN dt.team_id IS NOT NULL THEN dt.team_id
            WHEN COALESCE(apc.active_team_count, 0) = 1 THEN element_at(apc.active_team_ids, 1)
            WHEN COALESCE(apc.active_team_count, 0) > 1 THEN NULL
            WHEN COALESCE(hpc.historical_team_count, 0) = 1 THEN element_at(hpc.historical_team_ids, 1)
            WHEN COALESCE(hpc.historical_team_count, 0) > 1 THEN NULL
            ELSE NULL
        END AS resolved_team_id,
        CASE
            WHEN dt.team_id IS NOT NULL THEN '{DIRECT_VALID_TEAM_ID}'
            WHEN COALESCE(apc.active_team_count, 0) = 1 THEN '{ACTIVE_MEMBERSHIP_PAIR}'
            WHEN COALESCE(apc.active_team_count, 0) > 1 THEN '{AMBIGUOUS}'
            WHEN COALESCE(hpc.historical_team_count, 0) = 1 THEN '{UNIQUE_HISTORICAL_PAIR}'
            WHEN COALESCE(hpc.historical_team_count, 0) > 1 THEN '{AMBIGUOUS}'
            ELSE '{UNRESOLVED}'
        END AS team_resolution_method,
        CASE
            WHEN dt.team_id IS NOT NULL THEN '{RESOLVED}'
            WHEN COALESCE(apc.active_team_count, 0) = 1 THEN '{RESOLVED}'
            WHEN COALESCE(apc.active_team_count, 0) > 1 THEN '{AMBIGUOUS}'
            WHEN COALESCE(hpc.historical_team_count, 0) = 1 THEN '{RESOLVED}'
            WHEN COALESCE(hpc.historical_team_count, 0) > 1 THEN '{AMBIGUOUS}'
            ELSE '{UNRESOLVED}'
        END AS team_resolution_status,
        CASE
            WHEN dt.team_id IS NOT NULL THEN {DIRECT_RESOLUTION_CONFIDENCE}
            WHEN COALESCE(apc.active_team_count, 0) = 1 THEN {ACTIVE_PAIR_RESOLUTION_CONFIDENCE}
            WHEN COALESCE(hpc.historical_team_count, 0) = 1 THEN {HISTORICAL_PAIR_RESOLUTION_CONFIDENCE}
            ELSE {UNRESOLVED_CONFIDENCE}
        END AS team_resolution_confidence
    FROM base AS b
    LEFT JOIN teams_normalized AS dt
      ON b.direct_team_id = dt.team_id
     AND (b.match_date IS NULL OR dt.formation_date IS NULL OR b.match_date >= dt.formation_date)
     AND (b.match_date IS NULL OR dt.dissolution_date IS NULL OR b.match_date <= dt.dissolution_date)
    LEFT JOIN active_pair_candidates AS apc
      ON b.match_team_id = apc.match_team_id
    LEFT JOIN historical_pair_candidates AS hpc
      ON b.player_one_id = hpc.player_one_id
     AND b.player_two_id = hpc.player_two_id
)
SELECT
    match_id,
    match_team_id,
    team_number,
    match_date,
    player_one_id,
    player_two_id,
    canonical_player_pair_key,
    resolved_team_id,
    team_resolution_method,
    team_resolution_status,
    team_resolution_confidence,
    CASE
        WHEN rt.team_id IS NULL THEN FALSE
        WHEN rt.active_flag IS TRUE THEN TRUE
        WHEN rt.active_flag IS FALSE THEN FALSE
        WHEN rt.team_status = 'ACTIVE' THEN TRUE
        ELSE FALSE
    END AS candidate_attribution_allowed_flag
FROM resolved_pre AS rp
LEFT JOIN teams_normalized AS rt
  ON rp.resolved_team_id = rt.team_id
""".strip()


def publish_resolved_match_teams(
    spark: Any,
    environment: ReleaseEnvironment,
) -> ResolvedMatchTeamsPublicationSummary:
    """Build and publish resolved_match_teams using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "resolved_match_teams")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "resolved_match_teams")
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_resolved_match_teams_sql(environment),
        validation_fn=_validate_resolved_match_teams_table,
    )
    input_row_count = int(spark.table(get_silver_source_table_fqn(environment, "match_teams")).count())
    summary_row = spark.sql(
        f"""
SELECT
    COUNT(*) AS output_row_count,
    SUM(CASE WHEN team_resolution_method = '{DIRECT_VALID_TEAM_ID}' THEN 1 ELSE 0 END) AS direct_resolution_count,
    SUM(CASE WHEN team_resolution_method = '{ACTIVE_MEMBERSHIP_PAIR}' THEN 1 ELSE 0 END) AS active_pair_resolution_count,
    SUM(CASE WHEN team_resolution_method = '{UNIQUE_HISTORICAL_PAIR}' THEN 1 ELSE 0 END) AS historical_pair_resolution_count,
    SUM(CASE WHEN team_resolution_status = '{AMBIGUOUS}' THEN 1 ELSE 0 END) AS ambiguous_count,
    SUM(CASE WHEN team_resolution_status = '{UNRESOLVED}' THEN 1 ELSE 0 END) AS unresolved_count,
    CASE
        WHEN COUNT(*) = 0 THEN 0.0
        ELSE 100.0 * SUM(CASE WHEN team_resolution_status = '{RESOLVED}' THEN 1 ELSE 0 END) / COUNT(*)
    END AS persistent_team_resolution_pct
FROM {target_table_fqn}
""".strip()
    ).collect()[0]
    mapping = summary_row.asDict(recursive=True) if hasattr(summary_row, "asDict") else dict(summary_row)

    return ResolvedMatchTeamsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=int(mapping["output_row_count"]),
        direct_resolution_count=int(mapping["direct_resolution_count"] or 0),
        active_pair_resolution_count=int(mapping["active_pair_resolution_count"] or 0),
        historical_pair_resolution_count=int(mapping["historical_pair_resolution_count"] or 0),
        ambiguous_count=int(mapping["ambiguous_count"] or 0),
        unresolved_count=int(mapping["unresolved_count"] or 0),
        persistent_team_resolution_pct=float(mapping["persistent_team_resolution_pct"] or 0.0),
    )


def _validate_resolved_match_teams_table(spark: Any, table_fqn: str) -> None:
    """Validate primary-key uniqueness and non-null key fields for resolved_match_teams."""
    validation_row = spark.sql(
        f"""
SELECT
    SUM(CASE WHEN match_id IS NULL OR team_number IS NULL THEN 1 ELSE 0 END) AS null_key_count,
    SUM(CASE WHEN duplicate_key_count > 1 THEN 1 ELSE 0 END) AS duplicate_group_count
FROM (
    SELECT
        match_id,
        team_number,
        COUNT(*) AS duplicate_key_count
    FROM {table_fqn}
    GROUP BY match_id, team_number
)
""".strip()
    ).collect()[0]
    mapping = validation_row.asDict(recursive=True) if hasattr(validation_row, "asDict") else dict(validation_row)
    null_key_count = int(mapping["null_key_count"] or 0)
    duplicate_group_count = int(mapping["duplicate_group_count"] or 0)
    if null_key_count != 0 or duplicate_group_count != 0:
        raise ValueError(
            f"Resolved match teams validation failed for {table_fqn}: "
            f"null_key_count={null_key_count}, duplicate_group_count={duplicate_group_count}."
        )


def _resolve_match_team_row(
    match_team_row: dict[str, Any],
    *,
    teams_by_id: dict[str, dict[str, Any]],
    player_ids_by_match_team: dict[str, list[str]],
    active_pair_index: dict[tuple[str, str], list[tuple[str, date | None, date | None]]],
    historical_pair_index: dict[tuple[str, str], list[str]],
) -> dict[str, Any]:
    match_team_id = str(match_team_row["match_team_id"])
    match_id = str(match_team_row["match_id"])
    team_number = match_team_row.get("team_number")
    direct_team_id = _normalize_optional_string(match_team_row.get("team_id"))
    match_date = _parse_date_value(match_team_row.get("match_date"))
    player_ids = sorted(set(player_ids_by_match_team.get(match_team_id, [])))
    pair_key = _canonical_pair_key(player_ids)
    pair_tuple = _canonical_pair_tuple(player_ids)

    if direct_team_id is not None:
        direct_team = teams_by_id.get(direct_team_id)
        if direct_team is not None and _team_is_valid_for_match_date(direct_team, match_date):
            return _build_resolved_row(
                match_id=match_id,
                match_team_id=match_team_id,
                team_number=team_number,
                match_date=match_date,
                canonical_player_pair_key=pair_key,
                resolved_team_id=direct_team_id,
                team_resolution_method=DIRECT_VALID_TEAM_ID,
                team_resolution_status=RESOLVED,
                team_resolution_confidence=DIRECT_RESOLUTION_CONFIDENCE,
                candidate_attribution_allowed_flag=_is_candidate_attributable_team(direct_team),
            )

    if pair_tuple is None or match_date is None:
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=None,
            team_resolution_method=UNRESOLVED,
            team_resolution_status=UNRESOLVED,
            team_resolution_confidence=UNRESOLVED_CONFIDENCE,
            candidate_attribution_allowed_flag=False,
        )

    active_candidates = _resolve_active_pair_candidates(
        pair_tuple,
        match_date=match_date,
        active_pair_index=active_pair_index,
        teams_by_id=teams_by_id,
    )
    if len(active_candidates) == 1:
        resolved_team_id = active_candidates[0]
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=resolved_team_id,
            team_resolution_method=ACTIVE_MEMBERSHIP_PAIR,
            team_resolution_status=RESOLVED,
            team_resolution_confidence=ACTIVE_PAIR_RESOLUTION_CONFIDENCE,
            candidate_attribution_allowed_flag=_is_candidate_attributable_team(
                teams_by_id[resolved_team_id]
            ),
        )
    if len(active_candidates) > 1:
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=None,
            team_resolution_method=AMBIGUOUS,
            team_resolution_status=AMBIGUOUS,
            team_resolution_confidence=UNRESOLVED_CONFIDENCE,
            candidate_attribution_allowed_flag=False,
        )

    historical_candidates = historical_pair_index.get(pair_tuple, [])
    if len(historical_candidates) == 1:
        resolved_team_id = historical_candidates[0]
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=resolved_team_id,
            team_resolution_method=UNIQUE_HISTORICAL_PAIR,
            team_resolution_status=RESOLVED,
            team_resolution_confidence=HISTORICAL_PAIR_RESOLUTION_CONFIDENCE,
            candidate_attribution_allowed_flag=_is_candidate_attributable_team(
                teams_by_id.get(resolved_team_id, {})
            ),
        )
    if len(historical_candidates) > 1:
        return _build_resolved_row(
            match_id=match_id,
            match_team_id=match_team_id,
            team_number=team_number,
            match_date=match_date,
            canonical_player_pair_key=pair_key,
            resolved_team_id=None,
            team_resolution_method=AMBIGUOUS,
            team_resolution_status=AMBIGUOUS,
            team_resolution_confidence=UNRESOLVED_CONFIDENCE,
            candidate_attribution_allowed_flag=False,
        )

    return _build_resolved_row(
        match_id=match_id,
        match_team_id=match_team_id,
        team_number=team_number,
        match_date=match_date,
        canonical_player_pair_key=pair_key,
        resolved_team_id=None,
        team_resolution_method=UNRESOLVED,
        team_resolution_status=UNRESOLVED,
        team_resolution_confidence=UNRESOLVED_CONFIDENCE,
        candidate_attribution_allowed_flag=False,
    )


def _build_resolved_row(
    *,
    match_id: str,
    match_team_id: str,
    team_number: Any,
    match_date: date | None,
    canonical_player_pair_key: str | None,
    resolved_team_id: str | None,
    team_resolution_method: str,
    team_resolution_status: str,
    team_resolution_confidence: float,
    candidate_attribution_allowed_flag: bool,
) -> dict[str, Any]:
    return {
        "match_id": match_id,
        "match_team_id": match_team_id,
        "team_number": team_number,
        "match_date": match_date,
        "canonical_player_pair_key": canonical_player_pair_key,
        "resolved_team_id": resolved_team_id,
        "team_resolution_method": team_resolution_method,
        "team_resolution_status": team_resolution_status,
        "team_resolution_confidence": team_resolution_confidence,
        "candidate_attribution_allowed_flag": candidate_attribution_allowed_flag,
    }


def _group_player_ids_by_match_team(
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for row in match_team_players_rows:
        match_team_id = _normalize_optional_string(row.get("match_team_id"))
        player_id = _normalize_optional_string(row.get("player_id"))
        if match_team_id is None or player_id is None:
            continue
        grouped.setdefault(match_team_id, []).append(player_id)
    return grouped


def _build_active_pair_index(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_by_id: dict[str, dict[str, Any]],
) -> dict[tuple[str, str], list[tuple[str, date | None, date | None]]]:
    team_memberships = _group_memberships_by_team(team_memberships_rows)
    pair_index: dict[tuple[str, str], list[tuple[str, date | None, date | None]]] = {}

    for team_id, memberships in team_memberships.items():
        if team_id not in teams_by_id:
            continue
        unique_memberships = _dedupe_memberships(memberships)
        for first_index, first_membership in enumerate(unique_memberships):
            for second_membership in unique_memberships[first_index + 1 :]:
                first_player = _normalize_optional_string(first_membership.get("player_id"))
                second_player = _normalize_optional_string(second_membership.get("player_id"))
                if first_player is None or second_player is None or first_player == second_player:
                    continue
                overlap_start, overlap_end = _membership_overlap_window(
                    first_membership,
                    second_membership,
                )
                if overlap_start is None and overlap_end is None and (
                    _parse_date_value(first_membership.get("membership_start_date")) is not None
                    or _parse_date_value(second_membership.get("membership_start_date")) is not None
                ):
                    continue
                pair_index.setdefault(
                    _sorted_pair(first_player, second_player),
                    [],
                ).append((team_id, overlap_start, overlap_end))
    return pair_index


def _build_historical_pair_index(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[tuple[str, str], list[str]]:
    team_memberships = _group_memberships_by_team(team_memberships_rows)
    pair_index: dict[tuple[str, str], set[str]] = {}

    for team_id, memberships in team_memberships.items():
        unique_memberships = _dedupe_memberships(memberships)
        for first_index, first_membership in enumerate(unique_memberships):
            for second_membership in unique_memberships[first_index + 1 :]:
                first_player = _normalize_optional_string(first_membership.get("player_id"))
                second_player = _normalize_optional_string(second_membership.get("player_id"))
                if first_player is None or second_player is None or first_player == second_player:
                    continue
                overlap_start, overlap_end = _membership_overlap_window(
                    first_membership,
                    second_membership,
                )
                if overlap_start is None and overlap_end is None and (
                    _parse_date_value(first_membership.get("membership_start_date")) is not None
                    or _parse_date_value(second_membership.get("membership_start_date")) is not None
                ):
                    continue
                pair_index.setdefault(_sorted_pair(first_player, second_player), set()).add(team_id)

    return {
        pair_key: sorted(team_ids)
        for pair_key, team_ids in pair_index.items()
    }


def _resolve_active_pair_candidates(
    pair_key: tuple[str, str],
    *,
    match_date: date,
    active_pair_index: dict[tuple[str, str], list[tuple[str, date | None, date | None]]],
    teams_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    candidates: set[str] = set()
    for team_id, overlap_start, overlap_end in active_pair_index.get(pair_key, []):
        if overlap_start is not None and match_date < overlap_start:
            continue
        if overlap_end is not None and match_date > overlap_end:
            continue
        team_row = teams_by_id.get(team_id)
        if team_row is None or not _team_is_valid_for_match_date(team_row, match_date):
            continue
        candidates.add(team_id)
    return sorted(candidates)


def _group_memberships_by_team(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in team_memberships_rows:
        team_id = _normalize_optional_string(row.get("team_id"))
        if team_id is None:
            continue
        grouped.setdefault(team_id, []).append(row)
    return grouped


def _dedupe_memberships(memberships: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str | None, date | None, date | None]] = set()
    for membership in memberships:
        signature = (
            _normalize_optional_string(membership.get("player_id")),
            _parse_date_value(membership.get("membership_start_date")),
            _parse_date_value(membership.get("membership_end_date")),
        )
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(membership)
    return deduped


def _membership_overlap_window(
    first_membership: dict[str, Any],
    second_membership: dict[str, Any],
) -> tuple[date | None, date | None]:
    first_start = _parse_date_value(first_membership.get("membership_start_date"))
    first_end = _parse_date_value(first_membership.get("membership_end_date"))
    second_start = _parse_date_value(second_membership.get("membership_start_date"))
    second_end = _parse_date_value(second_membership.get("membership_end_date"))

    overlap_start = _max_date(first_start, second_start)
    overlap_end = _min_date(first_end, second_end)
    if overlap_start is not None and overlap_end is not None and overlap_start > overlap_end:
        return None, None
    return overlap_start, overlap_end


def _canonical_pair_key(player_ids: list[str]) -> str | None:
    if len(player_ids) != 2:
        return None
    return f"{player_ids[0]}:{player_ids[1]}"


def _canonical_pair_tuple(player_ids: list[str]) -> tuple[str, str] | None:
    if len(player_ids) != 2:
        return None
    return player_ids[0], player_ids[1]


def _sorted_pair(first_player: str, second_player: str) -> tuple[str, str]:
    return tuple(sorted((first_player, second_player)))  # type: ignore[return-value]


def _normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_date_value(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _team_is_valid_for_match_date(team_row: dict[str, Any], match_date: date | None) -> bool:
    if match_date is None:
        return True
    formation_date = _parse_date_value(team_row.get("formation_date"))
    dissolution_date = _parse_date_value(team_row.get("dissolution_date"))
    if formation_date is not None and match_date < formation_date:
        return False
    if dissolution_date is not None and match_date > dissolution_date:
        return False
    return True


def _is_candidate_attributable_team(team_row: dict[str, Any]) -> bool:
    if not team_row:
        return False
    active_flag = team_row.get("active_flag")
    if isinstance(active_flag, bool):
        return active_flag
    status = _normalize_optional_string(team_row.get("team_status"))
    if status is None:
        return False
    return status.upper() == "ACTIVE"


def _max_date(first_value: date | None, second_value: date | None) -> date | None:
    if first_value is None:
        return second_value
    if second_value is None:
        return first_value
    return max(first_value, second_value)


def _min_date(first_value: date | None, second_value: date | None) -> date | None:
    if first_value is None:
        return second_value
    if second_value is None:
        return first_value
    return min(first_value, second_value)
