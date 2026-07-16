"""Cross-table validation helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from napa_pipeline.bronze_to_silver.environment import ReleaseEnvironment
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


def run_cross_table_validations_sql(
    spark: Any,
    context: PipelineContext,
    environment: ReleaseEnvironment,
    *,
    expected_match_team_count: int,
    expected_match_team_player_count: int,
) -> CrossTableValidationResult:
    """Run the cross-table validations directly in Spark SQL."""
    teams_fqn = _silver_table_fqn(environment, "teams")
    team_memberships_fqn = _silver_table_fqn(environment, "team_memberships")
    matches_fqn = _silver_table_fqn(environment, "matches")
    match_teams_fqn = _silver_table_fqn(environment, "match_teams")
    match_team_players_fqn = _silver_table_fqn(environment, "match_team_players")
    match_games_fqn = _silver_table_fqn(environment, "match_games")
    players_fqn = _silver_table_fqn(environment, "players")

    checks = [
        {
            "target_table": "vw_team_rosters",
            "rule_id": "CROSS_TEAM_001",
            "rule_type": "cardinality",
            "severity": "WARNING",
            "evaluated_sql": f"SELECT COUNT(*) AS value FROM {teams_fqn} WHERE active_flag = true",
            "failed_sql": f"""
SELECT t.team_id AS business_key
FROM {teams_fqn} t
LEFT JOIN {team_memberships_fqn} tm
    ON t.team_id = tm.team_id
   AND tm.current_membership_flag = true
WHERE t.active_flag = true
GROUP BY t.team_id
HAVING COUNT(tm.team_membership_id) <> {expected_match_team_player_count}
""".strip(),
            "threshold_value": str(expected_match_team_player_count),
        },
        {
            "target_table": "matches",
            "rule_id": "CROSS_MATCH_001",
            "rule_type": "cardinality",
            "severity": "WARNING",
            "evaluated_sql": f"SELECT COUNT(*) AS value FROM {matches_fqn} WHERE completed_flag = true",
            "failed_sql": f"""
SELECT m.match_id AS business_key
FROM {matches_fqn} m
LEFT JOIN {match_teams_fqn} mt
    ON m.match_id = mt.match_id
WHERE m.completed_flag = true
GROUP BY m.match_id
HAVING COUNT(mt.match_team_id) <> {expected_match_team_count}
""".strip(),
            "threshold_value": str(expected_match_team_count),
        },
        {
            "target_table": "matches",
            "rule_id": "CROSS_MATCH_002",
            "rule_type": "cardinality",
            "severity": "WARNING",
            "evaluated_sql": f"SELECT COUNT(*) AS value FROM {matches_fqn} WHERE completed_flag = true",
            "failed_sql": f"""
SELECT m.match_id AS business_key
FROM {matches_fqn} m
LEFT JOIN {match_games_fqn} mg
    ON m.match_id = mg.match_id
WHERE m.completed_flag = true
GROUP BY m.match_id
HAVING COUNT(mg.match_game_id) < 1
""".strip(),
            "threshold_value": ">=1",
        },
        {
            "target_table": "match_team_players",
            "rule_id": "CROSS_MATCH_TEAM_001",
            "rule_type": "cardinality",
            "severity": "WARNING",
            "evaluated_sql": f"SELECT COUNT(*) AS value FROM {match_teams_fqn}",
            "failed_sql": f"""
SELECT mt.match_team_id AS business_key
FROM {match_teams_fqn} mt
LEFT JOIN {match_team_players_fqn} mtp
    ON mt.match_team_id = mtp.match_team_id
GROUP BY mt.match_team_id
HAVING COUNT(mtp.match_team_player_id) <> {expected_match_team_player_count}
""".strip(),
            "threshold_value": str(expected_match_team_player_count),
        },
        {
            "target_table": "team_memberships",
            "rule_id": "CROSS_PLAYER_001",
            "rule_type": "foreign_key",
            "severity": "ERROR",
            "evaluated_sql": f"SELECT COUNT(*) AS value FROM {team_memberships_fqn}",
            "failed_sql": f"""
SELECT tm.team_membership_id AS business_key
FROM {team_memberships_fqn} tm
LEFT ANTI JOIN {players_fqn} p
    ON tm.player_id = p.player_id
""".strip(),
            "threshold_value": None,
        },
        {
            "target_table": "match_team_players",
            "rule_id": "CROSS_PLAYER_002",
            "rule_type": "foreign_key",
            "severity": "ERROR",
            "evaluated_sql": f"SELECT COUNT(*) AS value FROM {match_team_players_fqn}",
            "failed_sql": f"""
SELECT mtp.match_team_player_id AS business_key
FROM {match_team_players_fqn} mtp
LEFT ANTI JOIN {players_fqn} p
    ON mtp.player_id = p.player_id
""".strip(),
            "threshold_value": None,
        },
        {
            "target_table": "matches",
            "rule_id": "CROSS_WINNER_001",
            "rule_type": "consistency",
            "severity": "ERROR",
            "evaluated_sql": f"SELECT COUNT(*) AS value FROM {matches_fqn}",
            "failed_sql": f"""
WITH team_winners AS (
    SELECT
        match_id,
        COLLECT_SET(CAST(team_number AS INT)) AS winner_team_numbers
    FROM {match_teams_fqn}
    WHERE winner_flag = true
    GROUP BY match_id
),
game_winner_counts AS (
    SELECT
        match_id,
        CAST(winning_team_number AS INT) AS winning_team_number,
        COUNT(*) AS game_wins
    FROM {match_games_fqn}
    WHERE winning_team_number IS NOT NULL
    GROUP BY match_id, CAST(winning_team_number AS INT)
),
game_winners AS (
    SELECT
        match_id,
        winning_team_number
    FROM (
        SELECT
            match_id,
            winning_team_number,
            ROW_NUMBER() OVER (
                PARTITION BY match_id
                ORDER BY game_wins DESC, winning_team_number ASC
            ) AS row_num
        FROM game_winner_counts
    )
    WHERE row_num = 1
)
SELECT m.match_id AS business_key
FROM {matches_fqn} m
LEFT JOIN team_winners tw
    ON m.match_id = tw.match_id
LEFT JOIN game_winners gw
    ON m.match_id = gw.match_id
WHERE m.winning_team_number IS NOT NULL
  AND (
      (SIZE(COALESCE(tw.winner_team_numbers, ARRAY())) > 0
       AND NOT ARRAY_CONTAINS(tw.winner_team_numbers, CAST(m.winning_team_number AS INT)))
      OR
      (gw.winning_team_number IS NOT NULL
       AND gw.winning_team_number <> CAST(m.winning_team_number AS INT))
  )
""".strip(),
            "threshold_value": None,
        },
    ]

    quality_results: list[dict[str, Any]] = []
    run_messages: list[dict[str, Any]] = []
    warning_count = 0
    failure_count = 0

    for check in checks:
        evaluated_row_count = _scalar_count(spark, check["evaluated_sql"])
        failed_row_count = _scalar_count(
            spark,
            f"SELECT COUNT(*) AS value FROM ({check['failed_sql']}) failed_rows",
        )
        sample_business_keys = _sample_business_keys(spark, check["failed_sql"])
        status = "PASSED" if failed_row_count == 0 else "FAILED"
        failure_pct = _failure_pct(failed_row_count, evaluated_row_count)
        quality_record = build_quality_result_record(
            context,
            target_table=str(check["target_table"]),
            rule_id=str(check["rule_id"]),
            rule_type=str(check["rule_type"]),
            severity=str(check["severity"]),
            status=status,
            evaluated_row_count=evaluated_row_count,
            failed_row_count=failed_row_count,
            failure_pct=failure_pct,
            threshold_value=check["threshold_value"],
            sample_business_keys=sample_business_keys,
        )
        quality_results.append(quality_record)

        if failed_row_count:
            level = "WARNING" if check["severity"] == "WARNING" else "ERROR"
            run_messages.append(
                build_run_message_record(
                    context,
                    message_level=level,
                    message_code=str(check["rule_id"]),
                    message_text=(
                        f"{check['target_table']} validation {check['rule_id']} "
                        f"found {failed_row_count} failing rows."
                    ),
                    target_table=str(check["target_table"]),
                )
            )

        if check["severity"] == "WARNING":
            warning_count += failed_row_count
        else:
            failure_count += failed_row_count

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


def _scalar_count(spark: Any, query: str) -> int:
    row = spark.sql(query).collect()[0]
    if hasattr(row, "asDict"):
        return int(row.asDict(recursive=True)["value"])
    if isinstance(row, dict):
        return int(row["value"])
    return int(row[0])


def _sample_business_keys(spark: Any, query: str) -> list[str]:
    rows = spark.sql(f"SELECT business_key FROM ({query}) failed_rows LIMIT 10").collect()
    results: list[str] = []
    for row in rows:
        if hasattr(row, "asDict"):
            results.append(str(row.asDict(recursive=True)["business_key"]))
        elif isinstance(row, dict):
            results.append(str(row["business_key"]))
        else:
            results.append(str(row[0]))
    return results


def _silver_table_fqn(environment: ReleaseEnvironment, table_name: str) -> str:
    return f"{environment.catalog}.{environment.silver_schema}.{table_name}"
