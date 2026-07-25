"""Phase 3 competition foundation builders for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.publish import publish_stage_to_gold_table


@dataclass(frozen=True)
class CompetitionMatchSidesResult:
    """Built Phase 3 match-side rows with basic exclusion counts."""

    rows: tuple[dict[str, Any], ...]
    included_match_count: int
    excluded_match_count: int


@dataclass(frozen=True)
class CompetitionPlayerMatchesResult:
    """Built Phase 3 player-match rows."""

    rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class CompetitionMatchSidesPublicationSummary:
    """Published-table summary for the Gold competition_match_sides build."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class CompetitionPlayerMatchesPublicationSummary:
    """Published-table summary for the Gold competition_player_matches build."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


def build_competition_match_sides(
    matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_games_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    regions_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    analysis_as_of_date: date,
) -> CompetitionMatchSidesResult:
    """Build one Gold competition row per valid match side up to the analysis date."""
    matches_by_id = _group_rows_by_key(matches_rows, "match_id")
    sides_by_match_id = _group_rows_by_key(match_teams_rows, "match_id")
    players_by_match_team_id = _group_rows_by_key(match_team_players_rows, "match_team_id")
    games_by_match_id = _group_rows_by_key(match_games_rows, "match_id")
    regions_by_id = {
        _normalize_optional_string(row.get("region_id")): row
        for row in regions_rows
        if _normalize_optional_string(row.get("region_id")) is not None
    }
    batches_by_id = {
        _normalize_optional_string(row.get("batch_id")): row
        for row in monthly_batches_rows
        if _normalize_optional_string(row.get("batch_id")) is not None
    }

    accepted_rows: list[dict[str, Any]] = []
    included_match_count = 0
    excluded_match_count = 0

    for match_id in sorted(matches_by_id):
        match_group = matches_by_id[match_id]
        if len(match_group) != 1:
            excluded_match_count += 1
            continue

        match_row = match_group[0]
        match_date = _parse_date_value(match_row.get("match_date"))
        if match_date is None or match_date > analysis_as_of_date:
            excluded_match_count += 1
            continue

        sides = _normalize_valid_match_sides(
            sides_by_match_id.get(match_id, []),
            players_by_match_team_id=players_by_match_team_id,
        )
        if sides is None:
            excluded_match_count += 1
            continue

        side_numbers = {side["team_number"] for side in sides}
        game_summary = _summarize_match_games(games_by_match_id.get(match_id, []))
        if game_summary is None:
            excluded_match_count += 1
            continue

        winner_team_number = _resolve_match_winner_team_number(match_row, game_summary)
        if winner_team_number is None or winner_team_number not in side_numbers:
            excluded_match_count += 1
            continue

        if game_summary["winning_team_number"] is not None and winner_team_number != game_summary["winning_team_number"]:
            excluded_match_count += 1
            continue

        region_id = _normalize_optional_string(match_row.get("region_id"))
        batch_id = _normalize_optional_string(match_row.get("batch_id"))
        region_row = regions_by_id.get(region_id)
        batch_row = batches_by_id.get(batch_id)

        for side in sides:
            opponent = sides[1] if side["team_number"] == sides[0]["team_number"] else sides[0]
            points_for, points_against = _points_for_side(
                team_number=side["team_number"],
                team_one_points=game_summary["team_one_points"],
                team_two_points=game_summary["team_two_points"],
            )
            games_won, games_lost = _games_for_side(
                team_number=side["team_number"],
                team_one_games_won=game_summary["team_one_games_won"],
                team_two_games_won=game_summary["team_two_games_won"],
            )
            player_ids = side["player_ids"]
            accepted_rows.append(
                {
                    "match_id": match_id,
                    "match_team_id": side["match_team_id"],
                    "match_date": match_date,
                    "batch_id": batch_id,
                    "batch_sequence": batch_row.get("batch_sequence") if batch_row else None,
                    "batch_date": _parse_date_value(batch_row.get("batch_date")) if batch_row else None,
                    "region_id": region_id,
                    "match_country_code": _normalize_optional_string(
                        region_row.get("country_code") if region_row else None
                    ),
                    "match_type": _normalize_optional_string(match_row.get("match_type")),
                    "competition_category": _normalize_optional_string(
                        match_row.get("competition_category")
                    ),
                    "team_number": side["team_number"],
                    "opponent_team_number": opponent["team_number"],
                    "team_id": side["team_id"],
                    "opponent_team_id": opponent["team_id"],
                    "winning_team_id": _normalize_optional_string(match_row.get("winning_team_id")),
                    "winning_team_number": winner_team_number,
                    "completed_flag": _coerce_bool(match_row.get("completed_flag")),
                    # Phase 3 uses game wins as the match-side score because Silver does not expose
                    # a separate side-level match score.
                    "side_score": games_won,
                    "opponent_score": games_lost,
                    "won_flag": side["team_number"] == winner_team_number,
                    "lost_flag": side["team_number"] != winner_team_number,
                    "games_won": games_won,
                    "games_lost": games_lost,
                    "game_differential": games_won - games_lost,
                    "points_for": points_for,
                    "points_against": points_against,
                    "point_differential": points_for - points_against,
                    "point_share": _safe_point_share(points_for, points_against),
                    "close_game_count": game_summary["close_game_count"],
                    "deciding_game_flag": game_summary["deciding_game_flag"],
                    "pre_match_team_rating": side["pre_match_team_rating"],
                    "opponent_pre_match_team_rating": opponent["pre_match_team_rating"],
                    "player_one_id": player_ids[0],
                    "player_two_id": player_ids[1],
                    "canonical_player_pair_key": f"{player_ids[0]}:{player_ids[1]}",
                    "side_cardinality_warning_flag": _coerce_bool(
                        side["row"].get("side_cardinality_warning_flag")
                    ),
                    "membership_history_warning_flag": any(
                        _coerce_bool(player_row.get("membership_history_warning_flag"))
                        for player_row in side["player_rows"]
                    ),
                }
            )
        included_match_count += 1

    accepted_rows.sort(key=lambda row: (row["match_date"], row["match_id"], int(row["team_number"])))
    return CompetitionMatchSidesResult(
        rows=tuple(accepted_rows),
        included_match_count=included_match_count,
        excluded_match_count=excluded_match_count,
    )


def build_competition_player_matches(
    competition_match_sides_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> CompetitionPlayerMatchesResult:
    """Build one Gold competition row per valid player participation."""
    player_rows_by_match_team_id = _group_rows_by_key(match_team_players_rows, "match_team_id")
    accepted_rows: list[dict[str, Any]] = []

    for side_row in sorted(
        competition_match_sides_rows,
        key=lambda row: (row["match_date"], row["match_id"], int(row["team_number"])),
    ):
        match_team_id = _normalize_optional_string(side_row.get("match_team_id"))
        if match_team_id is None:
            continue

        player_rows = [
            row
            for row in player_rows_by_match_team_id.get(match_team_id, [])
            if _normalize_optional_string(row.get("player_id")) is not None
        ]
        unique_player_rows = _dedupe_player_rows(player_rows)
        if len(unique_player_rows) != 2:
            continue

        ordered_player_rows = sorted(
            unique_player_rows,
            key=lambda row: (
                _player_position_sort_key(row.get("player_position")),
                _normalize_optional_string(row.get("player_id")) or "",
            ),
        )
        player_ids = [
            _normalize_optional_string(row.get("player_id"))
            for row in ordered_player_rows
        ]
        if any(player_id is None for player_id in player_ids):
            continue

        for index, player_row in enumerate(ordered_player_rows):
            partner_row = ordered_player_rows[1 - index]
            accepted_rows.append(
                {
                    "match_id": side_row["match_id"],
                    "match_team_id": match_team_id,
                    "match_date": side_row["match_date"],
                    "batch_id": side_row.get("batch_id"),
                    "batch_sequence": side_row.get("batch_sequence"),
                    "batch_date": side_row.get("batch_date"),
                    "region_id": side_row.get("region_id"),
                    "match_country_code": side_row.get("match_country_code"),
                    "match_type": side_row.get("match_type"),
                    "competition_category": side_row.get("competition_category"),
                    "team_number": side_row["team_number"],
                    "opponent_team_number": side_row.get("opponent_team_number"),
                    "team_id": side_row.get("team_id"),
                    "opponent_team_id": side_row.get("opponent_team_id"),
                    "player_id": _normalize_optional_string(player_row.get("player_id")),
                    "player_position": _normalize_optional_string(player_row.get("player_position")),
                    "partner_player_id": _normalize_optional_string(partner_row.get("player_id")),
                    "canonical_player_pair_key": side_row.get("canonical_player_pair_key"),
                    "won_flag": _coerce_bool(side_row.get("won_flag")),
                    "lost_flag": _coerce_bool(side_row.get("lost_flag")),
                    "side_score": side_row.get("side_score"),
                    "opponent_score": side_row.get("opponent_score"),
                    "games_won": side_row.get("games_won"),
                    "games_lost": side_row.get("games_lost"),
                    "game_differential": side_row.get("game_differential"),
                    "points_for": side_row.get("points_for"),
                    "points_against": side_row.get("points_against"),
                    "point_differential": side_row.get("point_differential"),
                    "point_share": side_row.get("point_share"),
                    "close_game_count": side_row.get("close_game_count"),
                    "deciding_game_flag": side_row.get("deciding_game_flag"),
                    "pre_match_player_rating": _coerce_float(
                        player_row.get("player_rating_at_match")
                    ),
                    "pre_match_partner_rating": _coerce_float(
                        partner_row.get("player_rating_at_match")
                    ),
                    "pre_match_team_rating": _coerce_float(
                        side_row.get("pre_match_team_rating")
                    ),
                    "pre_match_opponent_team_rating": _coerce_float(
                        side_row.get("opponent_pre_match_team_rating")
                    ),
                    "membership_history_warning_flag": _coerce_bool(
                        player_row.get("membership_history_warning_flag")
                    ),
                }
            )

    accepted_rows.sort(
        key=lambda row: (
            row["match_date"],
            row["match_id"],
            int(row["team_number"]),
            _player_position_sort_key(row.get("player_position")),
            row["player_id"] or "",
        )
    )
    return CompetitionPlayerMatchesResult(rows=tuple(accepted_rows))


def build_competition_match_sides_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
) -> str:
    """Return the Spark SQL used to build the Gold competition_match_sides table."""
    matches_fqn = get_silver_source_table_fqn(environment, "matches")
    match_teams_fqn = get_silver_source_table_fqn(environment, "match_teams")
    match_team_players_fqn = get_silver_source_table_fqn(environment, "match_team_players")
    match_games_fqn = get_silver_source_table_fqn(environment, "match_games")
    regions_fqn = get_silver_source_table_fqn(environment, "regions")
    monthly_batches_fqn = get_silver_source_table_fqn(environment, "monthly_batches")
    analysis_literal = analysis_as_of_date.isoformat()

    return f"""
WITH matches_deduped AS (
    SELECT
        CAST(match_id AS STRING) AS match_id,
        CAST(batch_id AS STRING) AS batch_id,
        CAST(region_id AS STRING) AS region_id,
        CAST(match_date AS DATE) AS match_date,
        UPPER(TRIM(CAST(match_type AS STRING))) AS match_type,
        UPPER(TRIM(CAST(competition_category AS STRING))) AS competition_category,
        CAST(winning_team_id AS STRING) AS winning_team_id,
        CAST(winning_team_number AS INT) AS winning_team_number,
        CAST(completed_flag AS BOOLEAN) AS completed_flag,
        COUNT(*) OVER (PARTITION BY CAST(match_id AS STRING)) AS match_row_count
    FROM {matches_fqn}
),
valid_match_records AS (
    SELECT
        match_id,
        batch_id,
        region_id,
        match_date,
        match_type,
        competition_category,
        winning_team_id,
        winning_team_number,
        completed_flag
    FROM matches_deduped
    WHERE match_row_count = 1
      AND match_date IS NOT NULL
      AND match_date <= DATE('{analysis_literal}')
),
side_players AS (
    SELECT
        CAST(mtp.match_team_id AS STRING) AS match_team_id,
        CAST(mtp.match_id AS STRING) AS match_id,
        sort_array(collect_set(CAST(mtp.player_id AS STRING))) AS player_ids,
        COUNT(DISTINCT CAST(mtp.player_id AS STRING)) AS player_count,
        MAX(CASE WHEN COALESCE(CAST(mtp.membership_history_warning_flag AS BOOLEAN), FALSE) THEN 1 ELSE 0 END)
            AS membership_history_warning_int
    FROM {match_team_players_fqn} AS mtp
    WHERE mtp.match_team_id IS NOT NULL
      AND mtp.player_id IS NOT NULL
    GROUP BY CAST(mtp.match_team_id AS STRING), CAST(mtp.match_id AS STRING)
),
match_sides AS (
    SELECT
        CAST(mt.match_id AS STRING) AS match_id,
        CAST(mt.match_team_id AS STRING) AS match_team_id,
        CAST(mt.team_id AS STRING) AS team_id,
        CAST(mt.team_number AS INT) AS team_number,
        CAST(mt.pre_match_team_rating AS DOUBLE) AS pre_match_team_rating,
        CAST(mt.side_cardinality_warning_flag AS BOOLEAN) AS side_cardinality_warning_flag,
        sp.player_ids,
        sp.player_count,
        CAST(sp.membership_history_warning_int AS BOOLEAN) AS membership_history_warning_flag
    FROM {match_teams_fqn} AS mt
    INNER JOIN side_players AS sp
      ON CAST(mt.match_team_id AS STRING) = sp.match_team_id
),
side_quality AS (
    SELECT
        ms.*,
        COUNT(*) OVER (PARTITION BY ms.match_id) AS side_row_count,
        COUNT(DISTINCT ms.team_number) OVER (PARTITION BY ms.match_id) AS distinct_team_number_count,
        array_distinct(flatten(collect_list(ms.player_ids) OVER (PARTITION BY ms.match_id))) AS match_player_ids
    FROM match_sides AS ms
),
valid_sides AS (
    SELECT
        *
    FROM side_quality
    WHERE side_row_count = 2
      AND distinct_team_number_count = 2
      AND player_count = 2
      AND size(match_player_ids) = 4
),
game_base AS (
    SELECT
        CAST(match_id AS STRING) AS match_id,
        CAST(team_one_score AS INT) AS team_one_score,
        CAST(team_two_score AS INT) AS team_two_score,
        CAST(winning_team_number AS INT) AS winning_team_number,
        COALESCE(CAST(close_game_flag AS BOOLEAN), FALSE) AS close_game_flag
    FROM {match_games_fqn}
),
valid_games AS (
    SELECT
        *
    FROM game_base
    WHERE (
            winning_team_number IS NULL
            OR (
                team_one_score IS NOT NULL
                AND team_two_score IS NOT NULL
                AND (
                    (winning_team_number = 1 AND team_one_score > team_two_score)
                    OR (winning_team_number = 2 AND team_two_score > team_one_score)
                )
            )
        )
),
game_summary AS (
    SELECT
        match_id,
        COALESCE(SUM(COALESCE(team_one_score, 0)), 0) AS team_one_points,
        COALESCE(SUM(COALESCE(team_two_score, 0)), 0) AS team_two_points,
        COALESCE(SUM(CASE WHEN winning_team_number = 1 THEN 1 ELSE 0 END), 0) AS team_one_games_won,
        COALESCE(SUM(CASE WHEN winning_team_number = 2 THEN 1 ELSE 0 END), 0) AS team_two_games_won,
        COALESCE(SUM(CASE WHEN close_game_flag THEN 1 ELSE 0 END), 0) AS close_game_count,
        CASE
            WHEN SUM(CASE WHEN winning_team_number = 1 THEN 1 ELSE 0 END)
               > SUM(CASE WHEN winning_team_number = 2 THEN 1 ELSE 0 END) THEN 1
            WHEN SUM(CASE WHEN winning_team_number = 2 THEN 1 ELSE 0 END)
               > SUM(CASE WHEN winning_team_number = 1 THEN 1 ELSE 0 END) THEN 2
            ELSE NULL
        END AS derived_winning_team_number,
        CASE
            WHEN COUNT(*) >= 3
             AND SUM(CASE WHEN winning_team_number = 1 THEN 1 ELSE 0 END) > 0
             AND SUM(CASE WHEN winning_team_number = 2 THEN 1 ELSE 0 END) > 0 THEN TRUE
            ELSE FALSE
        END AS deciding_game_flag
    FROM valid_games
    GROUP BY match_id
),
eligible_matches AS (
    SELECT
        vm.match_id,
        vm.batch_id,
        vm.region_id,
        vm.match_date,
        vm.match_type,
        vm.competition_category,
        vm.winning_team_id,
        CASE
            WHEN vm.winning_team_number IN (1, 2) THEN vm.winning_team_number
            ELSE gs.derived_winning_team_number
        END AS winning_team_number,
        vm.completed_flag,
        gs.team_one_points,
        gs.team_two_points,
        gs.team_one_games_won,
        gs.team_two_games_won,
        gs.close_game_count,
        gs.deciding_game_flag
    FROM valid_match_records AS vm
    INNER JOIN game_summary AS gs
      ON vm.match_id = gs.match_id
    WHERE COALESCE(vm.winning_team_number, gs.derived_winning_team_number) IN (1, 2)
      AND (
            vm.winning_team_number IS NULL
            OR gs.derived_winning_team_number IS NULL
            OR vm.winning_team_number = gs.derived_winning_team_number
        )
),
side_pairs AS (
    SELECT
        left_side.match_id,
        left_side.match_team_id,
        left_side.team_id,
        left_side.team_number,
        left_side.pre_match_team_rating,
        left_side.side_cardinality_warning_flag,
        left_side.membership_history_warning_flag,
        element_at(left_side.player_ids, 1) AS player_one_id,
        element_at(left_side.player_ids, 2) AS player_two_id,
        concat_ws(':', element_at(left_side.player_ids, 1), element_at(left_side.player_ids, 2))
            AS canonical_player_pair_key,
        right_side.team_id AS opponent_team_id,
        right_side.team_number AS opponent_team_number,
        right_side.pre_match_team_rating AS opponent_pre_match_team_rating
    FROM valid_sides AS left_side
    INNER JOIN valid_sides AS right_side
      ON left_side.match_id = right_side.match_id
     AND left_side.team_number <> right_side.team_number
)
SELECT
    sp.match_id,
    sp.match_team_id,
    em.match_date,
    em.batch_id,
    mb.batch_sequence,
    CAST(mb.batch_date AS DATE) AS batch_date,
    em.region_id,
    UPPER(TRIM(CAST(r.country_code AS STRING))) AS match_country_code,
    em.match_type,
    em.competition_category,
    sp.team_number,
    sp.opponent_team_number,
    sp.team_id,
    sp.opponent_team_id,
    em.winning_team_id,
    em.winning_team_number,
    em.completed_flag,
    CASE
        WHEN sp.team_number = 1 THEN em.team_one_games_won
        ELSE em.team_two_games_won
    END AS side_score,
    CASE
        WHEN sp.team_number = 1 THEN em.team_two_games_won
        ELSE em.team_one_games_won
    END AS opponent_score,
    CASE WHEN sp.team_number = em.winning_team_number THEN TRUE ELSE FALSE END AS won_flag,
    CASE WHEN sp.team_number = em.winning_team_number THEN FALSE ELSE TRUE END AS lost_flag,
    CASE
        WHEN sp.team_number = 1 THEN em.team_one_games_won
        ELSE em.team_two_games_won
    END AS games_won,
    CASE
        WHEN sp.team_number = 1 THEN em.team_two_games_won
        ELSE em.team_one_games_won
    END AS games_lost,
    CASE
        WHEN sp.team_number = 1 THEN em.team_one_games_won - em.team_two_games_won
        ELSE em.team_two_games_won - em.team_one_games_won
    END AS game_differential,
    CASE
        WHEN sp.team_number = 1 THEN em.team_one_points
        ELSE em.team_two_points
    END AS points_for,
    CASE
        WHEN sp.team_number = 1 THEN em.team_two_points
        ELSE em.team_one_points
    END AS points_against,
    CASE
        WHEN sp.team_number = 1 THEN em.team_one_points - em.team_two_points
        ELSE em.team_two_points - em.team_one_points
    END AS point_differential,
    CASE
        WHEN COALESCE(em.team_one_points, 0) + COALESCE(em.team_two_points, 0) = 0 THEN NULL
        WHEN sp.team_number = 1 THEN
            CAST(em.team_one_points AS DOUBLE) / CAST(em.team_one_points + em.team_two_points AS DOUBLE)
        ELSE
            CAST(em.team_two_points AS DOUBLE) / CAST(em.team_one_points + em.team_two_points AS DOUBLE)
    END AS point_share,
    em.close_game_count,
    em.deciding_game_flag,
    sp.pre_match_team_rating,
    sp.opponent_pre_match_team_rating,
    sp.player_one_id,
    sp.player_two_id,
    sp.canonical_player_pair_key,
    sp.side_cardinality_warning_flag,
    sp.membership_history_warning_flag
FROM side_pairs AS sp
INNER JOIN eligible_matches AS em
  ON sp.match_id = em.match_id
LEFT JOIN {regions_fqn} AS r
  ON em.region_id = CAST(r.region_id AS STRING)
LEFT JOIN {monthly_batches_fqn} AS mb
  ON em.batch_id = CAST(mb.batch_id AS STRING)
""".strip()


def build_competition_player_matches_sql(environment: ReleaseEnvironment) -> str:
    """Return the Spark SQL used to build the Gold competition_player_matches table."""
    competition_match_sides_fqn = get_gold_target_table_fqn(environment, "competition_match_sides")
    match_team_players_fqn = get_silver_source_table_fqn(environment, "match_team_players")

    return f"""
WITH side_rows AS (
    SELECT
        match_id,
        match_team_id,
        match_date,
        batch_id,
        batch_sequence,
        batch_date,
        region_id,
        match_country_code,
        match_type,
        competition_category,
        team_number,
        opponent_team_number,
        team_id,
        opponent_team_id,
        won_flag,
        lost_flag,
        side_score,
        opponent_score,
        games_won,
        games_lost,
        game_differential,
        points_for,
        points_against,
        point_differential,
        point_share,
        close_game_count,
        deciding_game_flag,
        pre_match_team_rating,
        opponent_pre_match_team_rating,
        canonical_player_pair_key
    FROM {competition_match_sides_fqn}
),
player_rows AS (
    SELECT DISTINCT
        CAST(match_team_id AS STRING) AS match_team_id,
        CAST(player_id AS STRING) AS player_id,
        UPPER(TRIM(CAST(player_position AS STRING))) AS player_position,
        CAST(player_rating_at_match AS DOUBLE) AS player_rating_at_match,
        CAST(membership_history_warning_flag AS BOOLEAN) AS membership_history_warning_flag
    FROM {match_team_players_fqn}
    WHERE match_team_id IS NOT NULL
      AND player_id IS NOT NULL
),
player_pairs AS (
    SELECT
        current_player.match_team_id,
        current_player.player_id,
        current_player.player_position,
        current_player.player_rating_at_match,
        current_player.membership_history_warning_flag,
        partner_player.player_id AS partner_player_id,
        partner_player.player_rating_at_match AS pre_match_partner_rating
    FROM player_rows AS current_player
    INNER JOIN player_rows AS partner_player
      ON current_player.match_team_id = partner_player.match_team_id
     AND current_player.player_id <> partner_player.player_id
)
SELECT
    sr.match_id,
    sr.match_team_id,
    sr.match_date,
    sr.batch_id,
    sr.batch_sequence,
    sr.batch_date,
    sr.region_id,
    sr.match_country_code,
    sr.match_type,
    sr.competition_category,
    sr.team_number,
    sr.opponent_team_number,
    sr.team_id,
    sr.opponent_team_id,
    pp.player_id,
    pp.player_position,
    pp.partner_player_id,
    sr.canonical_player_pair_key,
    sr.won_flag,
    sr.lost_flag,
    sr.side_score,
    sr.opponent_score,
    sr.games_won,
    sr.games_lost,
    sr.game_differential,
    sr.points_for,
    sr.points_against,
    sr.point_differential,
    sr.point_share,
    sr.close_game_count,
    sr.deciding_game_flag,
    pp.player_rating_at_match AS pre_match_player_rating,
    pp.pre_match_partner_rating,
    sr.pre_match_team_rating,
    sr.opponent_pre_match_team_rating AS pre_match_opponent_team_rating,
    pp.membership_history_warning_flag
FROM side_rows AS sr
INNER JOIN player_pairs AS pp
  ON sr.match_team_id = pp.match_team_id
""".strip()


def publish_competition_match_sides(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
) -> CompetitionMatchSidesPublicationSummary:
    """Build and publish competition_match_sides using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "competition_match_sides")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "competition_match_sides")
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_competition_match_sides_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
        ),
        validation_fn=_validate_competition_match_sides_table,
    )
    input_row_count = int(spark.table(get_silver_source_table_fqn(environment, "match_teams")).count())
    output_row_count = int(spark.table(target_table_fqn).count())
    return CompetitionMatchSidesPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
    )


def publish_competition_player_matches(
    spark: Any,
    environment: ReleaseEnvironment,
) -> CompetitionPlayerMatchesPublicationSummary:
    """Build and publish competition_player_matches using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "competition_player_matches")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "competition_player_matches")
    publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_competition_player_matches_sql(environment),
        validation_fn=_validate_competition_player_matches_table,
    )
    input_row_count = int(spark.table(get_gold_target_table_fqn(environment, "competition_match_sides")).count())
    output_row_count = int(spark.table(target_table_fqn).count())
    return CompetitionPlayerMatchesPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=input_row_count,
        output_row_count=output_row_count,
    )


def _validate_competition_match_sides_table(spark: Any, table_fqn: str) -> None:
    """Validate primary-key uniqueness and non-null key fields for competition_match_sides."""
    _validate_key_constraints(
        spark,
        table_fqn,
        key_columns=("match_id", "team_number"),
        label="competition match sides",
    )


def _validate_competition_player_matches_table(spark: Any, table_fqn: str) -> None:
    """Validate primary-key uniqueness and non-null key fields for competition_player_matches."""
    _validate_key_constraints(
        spark,
        table_fqn,
        key_columns=("match_id", "team_number", "player_id"),
        label="competition player matches",
    )


def _validate_key_constraints(
    spark: Any,
    table_fqn: str,
    *,
    key_columns: tuple[str, ...],
    label: str,
) -> None:
    null_conditions = " OR ".join(f"{column} IS NULL" for column in key_columns)
    grouping = ", ".join(key_columns)
    validation_row = spark.sql(
        f"""
SELECT
    SUM(CASE WHEN {null_conditions} THEN 1 ELSE 0 END) AS null_key_count,
    SUM(CASE WHEN duplicate_key_count > 1 THEN 1 ELSE 0 END) AS duplicate_group_count
FROM (
    SELECT
        {grouping},
        COUNT(*) AS duplicate_key_count
    FROM {table_fqn}
    GROUP BY {grouping}
)
""".strip()
    ).collect()[0]
    mapping = validation_row.asDict(recursive=True) if hasattr(validation_row, "asDict") else dict(validation_row)
    null_key_count = int(mapping["null_key_count"] or 0)
    duplicate_group_count = int(mapping["duplicate_group_count"] or 0)
    if null_key_count != 0 or duplicate_group_count != 0:
        raise ValueError(
            f"{label.title()} validation failed for {table_fqn}: "
            f"null_key_count={null_key_count}, duplicate_group_count={duplicate_group_count}."
        )


def _normalize_valid_match_sides(
    side_rows: list[dict[str, Any]],
    *,
    players_by_match_team_id: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]] | None:
    normalized: list[dict[str, Any]] = []
    team_numbers: set[int] = set()
    all_player_ids: set[str] = set()

    for side_row in side_rows:
        match_team_id = _normalize_optional_string(side_row.get("match_team_id"))
        team_number = _coerce_int(side_row.get("team_number"))
        if match_team_id is None or team_number not in (1, 2) or team_number in team_numbers:
            return None

        player_rows = _dedupe_player_rows(players_by_match_team_id.get(match_team_id, []))
        player_ids = sorted(
            player_id
            for player_id in (
                _normalize_optional_string(player_row.get("player_id"))
                for player_row in player_rows
            )
            if player_id is not None
        )
        if len(player_ids) != 2 or len(set(player_ids)) != 2:
            return None
        if all_player_ids.intersection(player_ids):
            return None

        all_player_ids.update(player_ids)
        team_numbers.add(team_number)
        normalized.append(
            {
                "row": side_row,
                "match_team_id": match_team_id,
                "team_id": _normalize_optional_string(side_row.get("team_id")),
                "team_number": team_number,
                "pre_match_team_rating": _coerce_float(side_row.get("pre_match_team_rating")),
                "player_ids": player_ids,
                "player_rows": player_rows,
            }
        )

    if len(normalized) != 2:
        return None
    return sorted(normalized, key=lambda side: side["team_number"])


def _summarize_match_games(
    game_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, Any] | None:
    team_one_points = 0
    team_two_points = 0
    team_one_games_won = 0
    team_two_games_won = 0
    close_game_count = 0
    valid_game_count = 0

    for game_row in game_rows:
        team_one_score = _coerce_int(game_row.get("team_one_score"))
        team_two_score = _coerce_int(game_row.get("team_two_score"))
        winning_team_number = _coerce_int(game_row.get("winning_team_number"))

        if winning_team_number is not None and winning_team_number not in (1, 2):
            return None
        if (
            winning_team_number is not None
            and team_one_score is not None
            and team_two_score is not None
            and (
                (winning_team_number == 1 and team_one_score <= team_two_score)
                or (winning_team_number == 2 and team_two_score <= team_one_score)
            )
        ):
            return None

        team_one_points += team_one_score or 0
        team_two_points += team_two_score or 0
        if _coerce_bool(game_row.get("close_game_flag")):
            close_game_count += 1

        if winning_team_number == 1:
            team_one_games_won += 1
            valid_game_count += 1
        elif winning_team_number == 2:
            team_two_games_won += 1
            valid_game_count += 1

    if valid_game_count == 0:
        return None

    derived_winner = 1 if team_one_games_won > team_two_games_won else 2 if team_two_games_won > team_one_games_won else None
    return {
        "team_one_points": team_one_points,
        "team_two_points": team_two_points,
        "team_one_games_won": team_one_games_won,
        "team_two_games_won": team_two_games_won,
        "winning_team_number": derived_winner,
        "close_game_count": close_game_count,
        "deciding_game_flag": valid_game_count >= 3 and team_one_games_won > 0 and team_two_games_won > 0,
    }


def _resolve_match_winner_team_number(
    match_row: dict[str, Any],
    game_summary: dict[str, Any],
) -> int | None:
    source_winner = _coerce_int(match_row.get("winning_team_number"))
    if source_winner is not None:
        if source_winner in (1, 2):
            return source_winner
        return None
    return game_summary["winning_team_number"]


def _group_rows_by_key(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    key_name: str,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key_value = _normalize_optional_string(row.get(key_name))
        if key_value is None:
            continue
        grouped.setdefault(key_value, []).append(row)
    return grouped


def _dedupe_player_rows(
    player_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None]] = set()
    for player_row in player_rows:
        signature = (
            _normalize_optional_string(player_row.get("player_id")),
            _normalize_optional_string(player_row.get("player_position")),
        )
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(player_row)
    return deduped


def _points_for_side(
    *,
    team_number: int,
    team_one_points: int,
    team_two_points: int,
) -> tuple[int, int]:
    if team_number == 1:
        return team_one_points, team_two_points
    return team_two_points, team_one_points


def _games_for_side(
    *,
    team_number: int,
    team_one_games_won: int,
    team_two_games_won: int,
) -> tuple[int, int]:
    if team_number == 1:
        return team_one_games_won, team_two_games_won
    return team_two_games_won, team_one_games_won


def _safe_point_share(points_for: int, points_against: int) -> float | None:
    total_points = points_for + points_against
    if total_points == 0:
        return None
    return points_for / total_points


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


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().upper()
    return text in {"TRUE", "1", "YES", "Y"}


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _player_position_sort_key(value: Any) -> tuple[int, str]:
    normalized = (_normalize_optional_string(value) or "").upper()
    mapping = {
        "LEFT": 1,
        "RIGHT": 2,
    }
    return mapping.get(normalized, 9), normalized
