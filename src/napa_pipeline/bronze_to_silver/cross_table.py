"""Cross-table validation helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.bronze_to_silver.operations import (
    PipelineContext,
    build_quality_result_record,
    build_run_message_record,
)


@dataclass(frozen=True)
class CrossTableValidationResult:
    """Durable quality and message outputs from cross-table validation."""

    quality_results: tuple[dict[str, Any], ...]
    run_messages: tuple[dict[str, Any], ...]
    warning_count: int
    failure_count: int


def run_cross_table_validations(
    context: PipelineContext,
    *,
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_games_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    expected_match_team_count: int,
    expected_match_team_player_count: int,
) -> CrossTableValidationResult:
    """Run the cross-table validations defined by the Bronze-to-Silver spec."""
    quality_results: list[dict[str, Any]] = []
    run_messages: list[dict[str, Any]] = []

    quality_results.extend(
        _validate_active_team_membership_counts(
            context,
            teams_rows=teams_rows,
            team_memberships_rows=team_memberships_rows,
            expected_count=expected_match_team_player_count,
        )
    )
    quality_results.extend(
        _validate_completed_match_structure(
            context,
            matches_rows=matches_rows,
            match_teams_rows=match_teams_rows,
            match_games_rows=match_games_rows,
            expected_match_team_count=expected_match_team_count,
        )
    )
    quality_results.extend(
        _validate_match_team_player_counts(
            context,
            match_teams_rows=match_teams_rows,
            match_team_players_rows=match_team_players_rows,
            expected_player_count=expected_match_team_player_count,
        )
    )
    quality_results.extend(
        _validate_referenced_players(
            context,
            players_rows=players_rows,
            team_memberships_rows=team_memberships_rows,
            match_team_players_rows=match_team_players_rows,
        )
    )
    quality_results.extend(
        _validate_winner_consistency(
            context,
            matches_rows=matches_rows,
            match_teams_rows=match_teams_rows,
            match_games_rows=match_games_rows,
        )
    )

    for result in quality_results:
        if result["failed_row_count"]:
            level = "WARNING" if result["severity"] == "WARNING" else "ERROR"
            run_messages.append(
                build_run_message_record(
                    context,
                    message_level=level,
                    message_code=result["rule_id"],
                    message_text=(
                        f"{result['target_table']} validation {result['rule_id']} "
                        f"found {result['failed_row_count']} failing rows."
                    ),
                    target_table=result["target_table"],
                )
            )

    warning_count = sum(
        int(result["failed_row_count"] or 0)
        for result in quality_results
        if result["severity"] == "WARNING"
    )
    failure_count = sum(
        int(result["failed_row_count"] or 0)
        for result in quality_results
        if result["severity"] != "WARNING"
    )
    return CrossTableValidationResult(
        quality_results=tuple(quality_results),
        run_messages=tuple(run_messages),
        warning_count=warning_count,
        failure_count=failure_count,
    )


def _validate_active_team_membership_counts(
    context: PipelineContext,
    *,
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    expected_count: int,
) -> list[dict[str, Any]]:
    active_team_ids = {
        str(row["team_id"])
        for row in teams_rows
        if row.get("active_flag") is True
    }
    membership_counts: dict[str, int] = {}
    for membership in team_memberships_rows:
        if membership.get("current_membership_flag") is True:
            team_id = str(membership["team_id"])
            membership_counts[team_id] = membership_counts.get(team_id, 0) + 1

    failing_team_ids = sorted(
        team_id
        for team_id in active_team_ids
        if membership_counts.get(team_id, 0) != expected_count
    )
    return [
        build_quality_result_record(
            context,
            target_table="vw_team_rosters",
            rule_id="CROSS_TEAM_001",
            rule_type="cardinality",
            severity="WARNING",
            status="PASSED" if not failing_team_ids else "FAILED",
            evaluated_row_count=len(active_team_ids),
            failed_row_count=len(failing_team_ids),
            failure_pct=_failure_pct(len(failing_team_ids), len(active_team_ids)),
            threshold_value=str(expected_count),
            sample_business_keys=failing_team_ids[:10],
        )
    ]


def _validate_completed_match_structure(
    context: PipelineContext,
    *,
    matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_games_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    expected_match_team_count: int,
) -> list[dict[str, Any]]:
    match_team_counts: dict[str, int] = {}
    for row in match_teams_rows:
        match_id = str(row["match_id"])
        match_team_counts[match_id] = match_team_counts.get(match_id, 0) + 1

    match_game_counts: dict[str, int] = {}
    for row in match_games_rows:
        match_id = str(row["match_id"])
        match_game_counts[match_id] = match_game_counts.get(match_id, 0) + 1

    completed_rows = [row for row in matches_rows if row.get("completed_flag") is True]
    bad_team_count = []
    bad_game_count = []
    for match_row in completed_rows:
        match_id = str(match_row["match_id"])
        if match_team_counts.get(match_id, 0) != expected_match_team_count:
            bad_team_count.append(match_id)
        if match_game_counts.get(match_id, 0) < 1:
            bad_game_count.append(match_id)

    return [
        build_quality_result_record(
            context,
            target_table="matches",
            rule_id="CROSS_MATCH_001",
            rule_type="cardinality",
            severity="WARNING",
            status="PASSED" if not bad_team_count else "FAILED",
            evaluated_row_count=len(completed_rows),
            failed_row_count=len(bad_team_count),
            failure_pct=_failure_pct(len(bad_team_count), len(completed_rows)),
            threshold_value=str(expected_match_team_count),
            sample_business_keys=bad_team_count[:10],
        ),
        build_quality_result_record(
            context,
            target_table="matches",
            rule_id="CROSS_MATCH_002",
            rule_type="cardinality",
            severity="WARNING",
            status="PASSED" if not bad_game_count else "FAILED",
            evaluated_row_count=len(completed_rows),
            failed_row_count=len(bad_game_count),
            failure_pct=_failure_pct(len(bad_game_count), len(completed_rows)),
            threshold_value=">=1",
            sample_business_keys=bad_game_count[:10],
        ),
    ]


def _validate_match_team_player_counts(
    context: PipelineContext,
    *,
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    expected_player_count: int,
) -> list[dict[str, Any]]:
    player_counts: dict[str, int] = {}
    for row in match_team_players_rows:
        match_team_id = str(row["match_team_id"])
        player_counts[match_team_id] = player_counts.get(match_team_id, 0) + 1

    failing_match_team_ids = sorted(
        str(row["match_team_id"])
        for row in match_teams_rows
        if player_counts.get(str(row["match_team_id"]), 0) != expected_player_count
    )
    return [
        build_quality_result_record(
            context,
            target_table="match_team_players",
            rule_id="CROSS_MATCH_TEAM_001",
            rule_type="cardinality",
            severity="WARNING",
            status="PASSED" if not failing_match_team_ids else "FAILED",
            evaluated_row_count=len(match_teams_rows),
            failed_row_count=len(failing_match_team_ids),
            failure_pct=_failure_pct(len(failing_match_team_ids), len(match_teams_rows)),
            threshold_value=str(expected_player_count),
            sample_business_keys=failing_match_team_ids[:10],
        )
    ]


def _validate_referenced_players(
    context: PipelineContext,
    *,
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> list[dict[str, Any]]:
    known_player_ids = {str(row["player_id"]) for row in players_rows}
    membership_orphans = sorted(
        str(row["team_membership_id"])
        for row in team_memberships_rows
        if str(row["player_id"]) not in known_player_ids
    )
    participant_orphans = sorted(
        str(row["match_team_player_id"])
        for row in match_team_players_rows
        if str(row["player_id"]) not in known_player_ids
    )
    return [
        build_quality_result_record(
            context,
            target_table="team_memberships",
            rule_id="CROSS_PLAYER_001",
            rule_type="foreign_key",
            severity="ERROR",
            status="PASSED" if not membership_orphans else "FAILED",
            evaluated_row_count=len(team_memberships_rows),
            failed_row_count=len(membership_orphans),
            failure_pct=_failure_pct(len(membership_orphans), len(team_memberships_rows)),
            sample_business_keys=membership_orphans[:10],
        ),
        build_quality_result_record(
            context,
            target_table="match_team_players",
            rule_id="CROSS_PLAYER_002",
            rule_type="foreign_key",
            severity="ERROR",
            status="PASSED" if not participant_orphans else "FAILED",
            evaluated_row_count=len(match_team_players_rows),
            failed_row_count=len(participant_orphans),
            failure_pct=_failure_pct(len(participant_orphans), len(match_team_players_rows)),
            sample_business_keys=participant_orphans[:10],
        ),
    ]


def _validate_winner_consistency(
    context: PipelineContext,
    *,
    matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_games_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> list[dict[str, Any]]:
    match_team_winners: dict[str, set[int]] = {}
    for row in match_teams_rows:
        if row.get("winner_flag") is True:
            match_team_winners.setdefault(str(row["match_id"]), set()).add(int(row["team_number"]))

    game_winner_counts: dict[str, dict[int, int]] = {}
    for row in match_games_rows:
        match_id = str(row["match_id"])
        winner = row.get("winning_team_number")
        if winner is None:
            continue
        winners = game_winner_counts.setdefault(match_id, {})
        winners[int(winner)] = winners.get(int(winner), 0) + 1

    failing_match_ids: list[str] = []
    for match_row in matches_rows:
        match_id = str(match_row["match_id"])
        match_winner = match_row.get("winning_team_number")
        if match_winner is None:
            continue
        team_winners = match_team_winners.get(match_id, set())
        game_winners = game_winner_counts.get(match_id, {})
        aggregate_game_winner = None
        if game_winners:
            aggregate_game_winner = max(game_winners.items(), key=lambda item: (item[1], -item[0]))[0]
        if team_winners and team_winners != {int(match_winner)}:
            failing_match_ids.append(match_id)
            continue
        if aggregate_game_winner is not None and aggregate_game_winner != int(match_winner):
            failing_match_ids.append(match_id)

    failing_match_ids = sorted(set(failing_match_ids))
    return [
        build_quality_result_record(
            context,
            target_table="matches",
            rule_id="CROSS_WINNER_001",
            rule_type="consistency",
            severity="ERROR",
            status="PASSED" if not failing_match_ids else "FAILED",
            evaluated_row_count=len(matches_rows),
            failed_row_count=len(failing_match_ids),
            failure_pct=_failure_pct(len(failing_match_ids), len(matches_rows)),
            sample_business_keys=failing_match_ids[:10],
        )
    ]


def _failure_pct(failed_count: int, evaluated_count: int) -> float | None:
    if evaluated_count == 0:
        return None
    return (failed_count / evaluated_count) * 100.0
