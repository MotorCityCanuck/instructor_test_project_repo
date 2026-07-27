"""Population, team, competition, and evidence fitness rules for Raw releases."""

from __future__ import annotations

from typing import Any

from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.models import CertificationRuleResult, InventoryCertificationResult


def evaluate_fitness_rules(
    spark: Any,
    config: CertificationConfig,
    inventory_result: InventoryCertificationResult,
) -> tuple[CertificationRuleResult, ...]:
    """Evaluate Phase 4 population and evidence fitness rules."""
    loaded_by_source = {
        source.source_name: source for source in inventory_result.loaded_sources
    }
    thresholds = config.profile_thresholds
    results: list[CertificationRuleResult] = []
    results.extend(_evaluate_active_player_rate(spark, loaded_by_source, thresholds))
    results.extend(_evaluate_viable_team_depth(spark, loaded_by_source, thresholds))
    results.extend(_evaluate_zero_match_active_player_rate(spark, loaded_by_source, thresholds))
    results.extend(_evaluate_rating_and_confidence_coverage(spark, loaded_by_source, thresholds))
    results.extend(_evaluate_development_history(spark, loaded_by_source, thresholds))
    results.extend(_evaluate_recent_team_evidence(spark, loaded_by_source, thresholds))
    return tuple(results)


def _evaluate_active_player_rate(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    player_source = loaded_by_source.get("player_master")
    if player_source is None or player_source.read_status == "UNREADABLE":
        return []

    player_columns = {field["column_name"] for field in player_source.schema_fields}
    active_expression = _resolve_active_player_expression(player_columns)
    if active_expression is None:
        return [
            _warning_rule(
                "RAW_ACTIVE_PLAYER_RATE",
                "Active player rate can be assessed",
                "Population and Lifecycle Fitness",
                "players",
                "No supported active-player field was available in player_master.",
            )
        ]

    query = f"""
SELECT
    COUNT(*) AS total_players,
    SUM(CASE WHEN {active_expression} THEN 1 ELSE 0 END) AS active_players
FROM {player_source.temp_view_name}
""".strip()
    metrics = _run_metric_query(spark, "ACTIVE_PLAYER_RATE", query)
    total_players = int(metrics.get("total_players", 0) or 0)
    active_players = int(metrics.get("active_players", 0) or 0)
    active_rate = (active_players / total_players) if total_players else 0.0
    threshold = float(thresholds["minimum_active_player_rate_blocker"])
    return [
        CertificationRuleResult(
            rule_id="RAW_ACTIVE_PLAYER_RATE",
            name="Active player rate meets minimum threshold",
            pillar="Population and Lifecycle Fitness",
            category="players",
            status="PASS" if active_rate >= threshold else "FAIL",
            severity="info" if active_rate >= threshold else "blocker",
            message=(
                f"Active player rate is {active_rate:.4f}."
                if active_rate >= threshold
                else (
                    f"Active player rate is {active_rate:.4f}, below the minimum "
                    f"threshold of {threshold:.4f}."
                )
            ),
            affected_count=max(total_players - active_players, 0),
            observed_value=round(active_rate, 6),
            expected_value=threshold,
        )
    ]


def _evaluate_viable_team_depth(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    team_source = loaded_by_source.get("teams")
    membership_source = loaded_by_source.get("team_memberships")
    if (
        team_source is None
        or membership_source is None
        or team_source.read_status == "UNREADABLE"
        or membership_source.read_status == "UNREADABLE"
    ):
        return []

    team_columns = {field["column_name"] for field in team_source.schema_fields}
    team_active_expression = _resolve_active_team_expression(team_columns)
    if team_active_expression is None:
        return [
            _warning_rule(
                "RAW_VIABLE_TEAM_DEPTH_BY_COUNTRY_DIVISION",
                "Viable team depth can be assessed",
                "Team and Partnership Fitness",
                "teams",
                "No supported active-team field was available in teams.",
            )
        ]

    threshold = int(thresholds["minimum_viable_teams_per_country_division"])
    query = f"""
WITH team_member_counts AS (
    SELECT
        team_id,
        COUNT(DISTINCT player_id) AS member_count
    FROM {membership_source.temp_view_name}
    GROUP BY team_id
),
active_teams AS (
    SELECT
        country_code,
        team_division,
        COUNT(*) AS active_team_count
    FROM {team_source.temp_view_name} AS t
    INNER JOIN team_member_counts AS m
        ON t.id = m.team_id
    WHERE {team_active_expression}
      AND m.member_count = 2
      AND country_code IS NOT NULL
      AND team_division IS NOT NULL
    GROUP BY country_code, team_division
)
SELECT COUNT(*) AS weak_cohorts
FROM active_teams
WHERE active_team_count < {threshold}
""".strip()
    weak_cohorts = int(_run_single_value_query(spark, "VIABLE_TEAM_DEPTH", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_VIABLE_TEAM_DEPTH_BY_COUNTRY_DIVISION",
            name="Active team depth by country and division meets minimum threshold",
            pillar="Team and Partnership Fitness",
            category="teams",
            status="PASS" if weak_cohorts == 0 else "FAIL",
            severity="info" if weak_cohorts == 0 else "blocker",
            message=(
                "All active country/division cohorts meet the minimum viable team depth."
                if weak_cohorts == 0
                else (
                    f"{weak_cohorts} active country/division cohorts fall below the minimum "
                    f"team-depth threshold of {threshold}."
                )
            ),
            affected_count=weak_cohorts,
            expected_value=threshold,
        )
    ]


def _evaluate_zero_match_active_player_rate(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    player_source = loaded_by_source.get("player_master")
    match_team_players = loaded_by_source.get("match_team_players")
    if (
        player_source is None
        or match_team_players is None
        or player_source.read_status == "UNREADABLE"
        or match_team_players.read_status == "UNREADABLE"
    ):
        return []

    player_columns = {field["column_name"] for field in player_source.schema_fields}
    active_expression = _resolve_active_player_expression(player_columns)
    if active_expression is None:
        return []

    threshold = float(thresholds["maximum_zero_match_active_player_rate"])
    query = f"""
WITH active_players AS (
    SELECT CAST(player_id AS STRING) AS player_id
    FROM {player_source.temp_view_name}
    WHERE {active_expression}
),
matched_players AS (
    SELECT DISTINCT CAST(player_id AS STRING) AS player_id
    FROM {match_team_players.temp_view_name}
)
SELECT
    COUNT(*) AS active_players,
    SUM(CASE WHEN mp.player_id IS NULL THEN 1 ELSE 0 END) AS zero_match_active_players
FROM active_players AS ap
LEFT JOIN matched_players AS mp
    ON ap.player_id = mp.player_id
""".strip()
    metrics = _run_metric_query(spark, "ZERO_MATCH_ACTIVE_PLAYERS", query)
    active_players = int(metrics.get("active_players", 0) or 0)
    zero_match_players = int(metrics.get("zero_match_active_players", 0) or 0)
    rate = (zero_match_players / active_players) if active_players else 0.0
    return [
        CertificationRuleResult(
            rule_id="RAW_ZERO_MATCH_ACTIVE_PLAYER_RATE",
            name="Zero-match active player rate stays below the maximum threshold",
            pillar="Competition and Evidence Fitness",
            category="players",
            status="PASS" if rate <= threshold else "FAIL",
            severity="info" if rate <= threshold else "error",
            message=(
                f"Zero-match active player rate is {rate:.4f}."
                if rate <= threshold
                else (
                    f"Zero-match active player rate is {rate:.4f}, above the maximum "
                    f"threshold of {threshold:.4f}."
                )
            ),
            affected_count=zero_match_players,
            observed_value=round(rate, 6),
            expected_value=threshold,
        )
    ]


def _evaluate_rating_and_confidence_coverage(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    player_source = loaded_by_source.get("player_master")
    if player_source is None or player_source.read_status == "UNREADABLE":
        return []

    player_columns = {field["column_name"] for field in player_source.schema_fields}
    rating_column = _resolve_first_column(player_columns, ("rating", "player_rating"))
    confidence_column = _resolve_first_column(
        player_columns, ("rating_confidence", "confidence")
    )
    if rating_column is None:
        return []

    query = f"""
SELECT
    COUNT(*) AS total_players,
    SUM(CASE WHEN {rating_column} IS NOT NULL THEN 1 ELSE 0 END) AS rated_players,
    SUM(CASE WHEN {confidence_column} IS NOT NULL THEN 1 ELSE 0 END) AS confidence_players
FROM {player_source.temp_view_name}
""".strip()
    metrics = _run_metric_query(spark, "RATING_COVERAGE", query)
    total_players = int(metrics.get("total_players", 0) or 0)
    rated_players = int(metrics.get("rated_players", 0) or 0)
    confidence_players = int(metrics.get("confidence_players", 0) or 0)
    rating_rate = (rated_players / total_players) if total_players else 0.0
    confidence_rate = (confidence_players / total_players) if total_players else 0.0

    rating_threshold = float(thresholds["minimum_player_rating_coverage_rate"])
    confidence_threshold = float(thresholds["minimum_confidence_coverage_rate"])
    results = [
        CertificationRuleResult(
            rule_id="RAW_PLAYER_RATING_COVERAGE",
            name="Player rating coverage meets minimum threshold",
            pillar="Ratings, Confidence, and Development Fitness",
            category="ratings",
            status="PASS" if rating_rate >= rating_threshold else "FAIL",
            severity="info" if rating_rate >= rating_threshold else "error",
            message=(
                f"Player rating coverage is {rating_rate:.4f}."
                if rating_rate >= rating_threshold
                else (
                    f"Player rating coverage is {rating_rate:.4f}, below the minimum "
                    f"threshold of {rating_threshold:.4f}."
                )
            ),
            affected_count=max(total_players - rated_players, 0),
            observed_value=round(rating_rate, 6),
            expected_value=rating_threshold,
        )
    ]
    if confidence_column is not None:
        results.append(
            CertificationRuleResult(
                rule_id="RAW_PLAYER_CONFIDENCE_COVERAGE",
                name="Player confidence coverage meets minimum threshold",
                pillar="Ratings, Confidence, and Development Fitness",
                category="ratings",
                status="PASS" if confidence_rate >= confidence_threshold else "FAIL",
                severity="info" if confidence_rate >= confidence_threshold else "warning",
                message=(
                    f"Player confidence coverage is {confidence_rate:.4f}."
                    if confidence_rate >= confidence_threshold
                    else (
                        f"Player confidence coverage is {confidence_rate:.4f}, below the minimum "
                        f"threshold of {confidence_threshold:.4f}."
                    )
                ),
                affected_count=max(total_players - confidence_players, 0),
                observed_value=round(confidence_rate, 6),
                expected_value=confidence_threshold,
            )
        )
    return results


def _evaluate_development_history(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    assessment_source = loaded_by_source.get("player_assessment_history")
    if assessment_source is None or assessment_source.read_status == "UNREADABLE":
        return []

    threshold = int(thresholds["minimum_assessment_periods_for_development_probe"])
    minimum_players = int(thresholds["minimum_development_players_with_history"])
    query = f"""
WITH players_with_history AS (
    SELECT CAST(player_id AS STRING) AS player_id
    FROM {assessment_source.temp_view_name}
    GROUP BY CAST(player_id AS STRING)
    HAVING COUNT(*) >= {threshold}
)
SELECT COUNT(*) AS value
FROM players_with_history
""".strip()
    players_with_history = int(_run_single_value_query(spark, "DEVELOPMENT_HISTORY", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_DEVELOPMENT_HISTORY_COVERAGE",
            name="Sufficient players have longitudinal assessment history",
            pillar="Ratings, Confidence, and Development Fitness",
            category="development",
            status="PASS" if players_with_history >= minimum_players else "FAIL",
            severity="info" if players_with_history >= minimum_players else "error",
            message=(
                f"{players_with_history} players meet the longitudinal assessment threshold."
                if players_with_history >= minimum_players
                else (
                    f"Only {players_with_history} players meet the longitudinal assessment "
                    f"threshold of {threshold} periods; minimum required players is {minimum_players}."
                )
            ),
            affected_count=max(minimum_players - players_with_history, 0),
            observed_value=players_with_history,
            expected_value=minimum_players,
        )
    ]


def _evaluate_recent_team_evidence(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    match_teams = loaded_by_source.get("match_teams")
    if match_teams is None or match_teams.read_status == "UNREADABLE":
        return []

    threshold = int(thresholds["minimum_recent_matches_per_candidate_team"])
    query = f"""
WITH team_match_counts AS (
    SELECT CAST(team_id AS STRING) AS team_id, COUNT(*) AS match_count
    FROM {match_teams.temp_view_name}
    WHERE team_id IS NOT NULL
    GROUP BY CAST(team_id AS STRING)
)
SELECT COUNT(*) AS value
FROM team_match_counts
WHERE match_count >= {threshold}
""".strip()
    teams_with_evidence = int(_run_single_value_query(spark, "RECENT_TEAM_EVIDENCE", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_RECENT_TEAM_EVIDENCE",
            name="At least one team meets the recent match evidence threshold",
            pillar="Competition and Evidence Fitness",
            category="teams",
            status="PASS" if teams_with_evidence > 0 else "FAIL",
            severity="info" if teams_with_evidence > 0 else "error",
            message=(
                f"{teams_with_evidence} teams meet the recent match evidence threshold."
                if teams_with_evidence > 0
                else (
                    f"No teams meet the recent match evidence threshold of {threshold} matches."
                )
            ),
            affected_count=0 if teams_with_evidence > 0 else 1,
            observed_value=teams_with_evidence,
            expected_value=threshold,
        )
    ]


def _resolve_active_player_expression(player_columns: set[str]) -> str | None:
    if "active_flag" in player_columns:
        return "active_flag = true"
    status_column = _resolve_first_column(player_columns, ("player_status", "status"))
    if status_column is not None:
        return f"UPPER(TRIM(CAST({status_column} AS STRING))) = 'ACTIVE'"
    return None


def _resolve_active_team_expression(team_columns: set[str]) -> str | None:
    if "active_flag" in team_columns:
        return "active_flag = true"
    if "team_status" in team_columns:
        return "UPPER(TRIM(CAST(team_status AS STRING))) = 'ACTIVE'"
    return None


def _resolve_first_column(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for column_name in candidates:
        if column_name in columns:
            return column_name
    return None


def _warning_rule(
    rule_id: str,
    name: str,
    pillar: str,
    category: str,
    message: str,
) -> CertificationRuleResult:
    return CertificationRuleResult(
        rule_id=rule_id,
        name=name,
        pillar=pillar,
        category=category,
        status="WARN",
        severity="warning",
        message=message,
    )


def _run_metric_query(spark: Any, query_tag: str, sql_text: str) -> dict[str, Any]:
    query = f"/* {query_tag} */\n{sql_text}"
    row = spark.sql(query).collect()[0]
    return row.asDict() if hasattr(row, "asDict") else dict(row)


def _run_single_value_query(spark: Any, query_tag: str, sql_text: str) -> int:
    metrics = _run_metric_query(spark, query_tag, sql_text)
    return int(metrics.get("value", 0) or 0)
