"""Assignment pathway readiness probes for Raw certification."""

from __future__ import annotations

from typing import Any

from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.models import CertificationRuleResult, InventoryCertificationResult


TARGET_COUNTRIES = ("USA", "CAN")
TARGET_DIVISIONS = ("mens_doubles", "womens_doubles", "mixed_doubles")


def evaluate_assignment_probes(
    spark: Any,
    config: CertificationConfig,
    inventory_result: InventoryCertificationResult,
    structural_results: tuple[CertificationRuleResult, ...] | None = None,
) -> tuple[CertificationRuleResult, ...]:
    """Evaluate simplified analytical readiness probes without producing final answers."""
    loaded_by_source = {
        source.source_name: source for source in inventory_result.loaded_sources
    }
    thresholds = config.profile_thresholds
    structural_results = structural_results or ()
    results: list[CertificationRuleResult] = []
    results.extend(_national_ranking_probe(spark, loaded_by_source, thresholds))
    results.extend(_olympic_player_selection_probe(spark, loaded_by_source, thresholds))
    results.extend(_olympic_team_selection_probe(spark, loaded_by_source, thresholds))
    results.extend(_partnership_analysis_probe(spark, loaded_by_source, thresholds))
    results.extend(_future_development_probe(spark, loaded_by_source, thresholds))
    results.extend(_tournament_candidate_probe(spark, loaded_by_source, thresholds))
    results.extend(_data_quality_learning_probe(structural_results, thresholds))
    return tuple(results)


def _national_ranking_probe(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    player_source = loaded_by_source.get("player_master")
    if player_source is None or player_source.read_status == "UNREADABLE":
        return []

    player_columns = {field["column_name"] for field in player_source.schema_fields}
    country_column = _resolve_first_column(player_columns, ("country_code",))
    rating_column = _resolve_first_column(player_columns, ("rating", "player_rating"))
    if country_column is None or rating_column is None:
        return [
            _warning_rule(
                "RAW_PROBE_NATIONAL_RANKING",
                "National ranking probe can be evaluated",
                "Assignment Pathway Readiness",
                "rankings",
                "Required country or rating fields are unavailable for the ranking probe.",
            )
        ]

    threshold = int(thresholds["minimum_ranking_players_per_country"])
    country_filter = _in_list(TARGET_COUNTRIES)
    query = f"""
WITH ranked_country_players AS (
    SELECT UPPER(TRIM(CAST({country_column} AS STRING))) AS country_code
    FROM {player_source.temp_view_name}
    WHERE {country_column} IS NOT NULL
      AND {rating_column} IS NOT NULL
      AND UPPER(TRIM(CAST({country_column} AS STRING))) IN ({country_filter})
),
country_counts AS (
    SELECT country_code, COUNT(*) AS player_count
    FROM ranked_country_players
    GROUP BY country_code
)
SELECT COUNT(*) AS value
FROM country_counts
WHERE player_count < {threshold}
""".strip()
    weak_countries = int(_run_single_value_query(spark, "PROBE_NATIONAL_RANKING", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_PROBE_NATIONAL_RANKING",
            name="National ranking pathway has sufficient rated players by country",
            pillar="Assignment Pathway Readiness",
            category="rankings",
            status="PASS" if weak_countries == 0 else "FAIL",
            severity="info" if weak_countries == 0 else "error",
            message=(
                "National ranking probe passed for configured countries."
                if weak_countries == 0
                else (
                    f"{weak_countries} countries fall below the minimum ranking-player "
                    f"threshold of {threshold}."
                )
            ),
            affected_count=weak_countries,
            expected_value=threshold,
        )
    ]


def _olympic_player_selection_probe(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    player_source = loaded_by_source.get("player_master")
    if player_source is None or player_source.read_status == "UNREADABLE":
        return []

    player_columns = {field["column_name"] for field in player_source.schema_fields}
    country_column = _resolve_first_column(player_columns, ("country_code",))
    rating_column = _resolve_first_column(player_columns, ("rating", "player_rating"))
    confidence_column = _resolve_first_column(
        player_columns, ("rating_confidence", "confidence", "confidence_score")
    )
    division_column = _resolve_first_column(player_columns, ("preferred_division", "team_division", "division"))
    if country_column is None or rating_column is None:
        return [
            _warning_rule(
                "RAW_PROBE_OLYMPIC_PLAYER_SELECTION",
                "Olympic player-selection probe can be evaluated",
                "Assignment Pathway Readiness",
                "player_selection",
                "Required country or rating fields are unavailable for the player-selection probe.",
            )
        ]

    threshold = int(thresholds["minimum_player_candidates_per_country_division"])
    alternate_threshold = int(thresholds["minimum_alternate_players_per_country_division"])
    country_filter = _in_list(TARGET_COUNTRIES)
    division_projection = (
        f"LOWER(TRIM(CAST({division_column} AS STRING))) AS division_value"
        if division_column is not None
        else "'open_doubles' AS division_value"
    )
    confidence_predicate = (
        f"AND {confidence_column} IS NOT NULL" if confidence_column is not None else ""
    )
    division_filter = _in_list(TARGET_DIVISIONS)
    query = f"""
WITH candidate_players AS (
    SELECT
        UPPER(TRIM(CAST({country_column} AS STRING))) AS country_code,
        {division_projection}
    FROM {player_source.temp_view_name}
    WHERE {country_column} IS NOT NULL
      AND {rating_column} IS NOT NULL
      {confidence_predicate}
      AND UPPER(TRIM(CAST({country_column} AS STRING))) IN ({country_filter})
),
cohorts AS (
    SELECT country_code, division_value, COUNT(*) AS player_count
    FROM candidate_players
    WHERE division_value IN ({division_filter})
    GROUP BY country_code, division_value
)
SELECT COUNT(*) AS value
FROM cohorts
WHERE player_count < {threshold}
   OR player_count < ({threshold} + {alternate_threshold})
""".strip()
    weak_cohorts = int(_run_single_value_query(spark, "PROBE_PLAYER_SELECTION", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_PROBE_OLYMPIC_PLAYER_SELECTION",
            name="Olympic player-selection pathway has candidate and alternate depth",
            pillar="Assignment Pathway Readiness",
            category="player_selection",
            status="PASS" if weak_cohorts == 0 else "FAIL",
            severity="info" if weak_cohorts == 0 else "error",
            message=(
                "Olympic player-selection probe passed for configured country/division cohorts."
                if weak_cohorts == 0
                else (
                    f"{weak_cohorts} country/division cohorts fall below the configured "
                    "player candidate or alternate depth thresholds."
                )
            ),
            affected_count=weak_cohorts,
            expected_value=f"{threshold}+{alternate_threshold}",
        )
    ]


def _olympic_team_selection_probe(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    team_source = loaded_by_source.get("teams")
    memberships = loaded_by_source.get("team_memberships")
    match_teams = loaded_by_source.get("match_teams")
    if (
        team_source is None
        or memberships is None
        or match_teams is None
        or team_source.read_status == "UNREADABLE"
        or memberships.read_status == "UNREADABLE"
        or match_teams.read_status == "UNREADABLE"
    ):
        return []

    threshold = int(thresholds["minimum_distinct_candidate_teams_per_country_division"])
    evidence_threshold = int(thresholds["minimum_recent_matches_per_candidate_team"])
    country_filter = _in_list(TARGET_COUNTRIES)
    division_filter = _in_list(TARGET_DIVISIONS)
    query = f"""
WITH team_member_counts AS (
    SELECT team_id, COUNT(DISTINCT player_id) AS member_count
    FROM {memberships.temp_view_name}
    GROUP BY team_id
),
team_match_counts AS (
    SELECT CAST(team_id AS STRING) AS team_id, COUNT(*) AS match_count
    FROM {match_teams.temp_view_name}
    WHERE team_id IS NOT NULL
    GROUP BY CAST(team_id AS STRING)
),
candidate_teams AS (
    SELECT
        UPPER(TRIM(CAST(t.country_code AS STRING))) AS country_code,
        LOWER(TRIM(CAST(t.team_division AS STRING))) AS division_value,
        COUNT(*) AS candidate_team_count
    FROM {team_source.temp_view_name} AS t
    INNER JOIN team_member_counts AS m
        ON CAST(t.id AS STRING) = CAST(m.team_id AS STRING)
    INNER JOIN team_match_counts AS mc
        ON CAST(t.id AS STRING) = mc.team_id
    WHERE UPPER(TRIM(CAST(t.team_status AS STRING))) = 'ACTIVE'
      AND m.member_count = 2
      AND mc.match_count >= {evidence_threshold}
      AND UPPER(TRIM(CAST(t.country_code AS STRING))) IN ({country_filter})
      AND LOWER(TRIM(CAST(t.team_division AS STRING))) IN ({division_filter})
    GROUP BY
        UPPER(TRIM(CAST(t.country_code AS STRING))),
        LOWER(TRIM(CAST(t.team_division AS STRING)))
)
SELECT COUNT(*) AS value
FROM candidate_teams
WHERE candidate_team_count < {threshold}
""".strip()
    weak_cohorts = int(_run_single_value_query(spark, "PROBE_TEAM_SELECTION", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_PROBE_OLYMPIC_TEAM_SELECTION",
            name="Olympic team-selection pathway has viable candidate team depth",
            pillar="Assignment Pathway Readiness",
            category="team_selection",
            status="PASS" if weak_cohorts == 0 else "FAIL",
            severity="info" if weak_cohorts == 0 else "blocker",
            message=(
                "Olympic team-selection probe passed for configured country/division cohorts."
                if weak_cohorts == 0
                else (
                    f"{weak_cohorts} country/division cohorts fall below the configured "
                    f"candidate team depth threshold of {threshold}."
                )
            ),
            affected_count=weak_cohorts,
            expected_value=threshold,
        )
    ]


def _partnership_analysis_probe(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    team_source = loaded_by_source.get("teams")
    memberships = loaded_by_source.get("team_memberships")
    match_teams = loaded_by_source.get("match_teams")
    if (
        team_source is None
        or memberships is None
        or match_teams is None
        or team_source.read_status == "UNREADABLE"
        or memberships.read_status == "UNREADABLE"
        or match_teams.read_status == "UNREADABLE"
    ):
        return []

    threshold = int(thresholds["minimum_partnerships_with_repeat_matches"])
    query = f"""
WITH team_member_counts AS (
    SELECT team_id, COUNT(DISTINCT player_id) AS member_count
    FROM {memberships.temp_view_name}
    GROUP BY team_id
),
team_match_counts AS (
    SELECT CAST(team_id AS STRING) AS team_id, COUNT(*) AS match_count
    FROM {match_teams.temp_view_name}
    WHERE team_id IS NOT NULL
    GROUP BY CAST(team_id AS STRING)
)
SELECT COUNT(*) AS value
FROM team_match_counts AS mc
INNER JOIN team_member_counts AS tm
    ON CAST(tm.team_id AS STRING) = mc.team_id
WHERE tm.member_count = 2
  AND mc.match_count >= 2
""".strip()
    repeat_partnerships = int(_run_single_value_query(spark, "PROBE_PARTNERSHIP", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_PROBE_PARTNERSHIP_ANALYSIS",
            name="Partnership analysis pathway has repeated partnership evidence",
            pillar="Assignment Pathway Readiness",
            category="partnerships",
            status="PASS" if repeat_partnerships >= threshold else "FAIL",
            severity="info" if repeat_partnerships >= threshold else "error",
            message=(
                f"{repeat_partnerships} partnerships have repeat match evidence."
                if repeat_partnerships >= threshold
                else (
                    f"Only {repeat_partnerships} partnerships have repeat match evidence; "
                    f"minimum required is {threshold}."
                )
            ),
            affected_count=max(threshold - repeat_partnerships, 0),
            observed_value=repeat_partnerships,
            expected_value=threshold,
        )
    ]


def _future_development_probe(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    assessments = loaded_by_source.get("player_assessment_history")
    players = loaded_by_source.get("player_master")
    if (
        assessments is None
        or players is None
        or assessments.read_status == "UNREADABLE"
        or players.read_status == "UNREADABLE"
    ):
        return []

    player_columns = {field["column_name"] for field in players.schema_fields}
    country_column = _resolve_first_column(player_columns, ("country_code",))
    if country_column is None:
        return []

    threshold = int(thresholds["minimum_development_players_with_history"])
    assessment_period_threshold = int(thresholds["minimum_assessment_periods_for_development_probe"])
    country_filter = _in_list(TARGET_COUNTRIES)
    query = f"""
WITH assessment_players AS (
    SELECT
        CAST(a.player_id AS STRING) AS player_id,
        COUNT(*) AS assessment_count
    FROM {assessments.temp_view_name} AS a
    GROUP BY CAST(a.player_id AS STRING)
    HAVING COUNT(*) >= {assessment_period_threshold}
),
country_players AS (
    SELECT CAST(player_id AS STRING) AS player_id
    FROM {players.temp_view_name}
    WHERE country_code IS NOT NULL
      AND UPPER(TRIM(CAST({country_column} AS STRING))) IN ({country_filter})
)
SELECT COUNT(*) AS value
FROM assessment_players AS a
INNER JOIN country_players AS p
    ON a.player_id = p.player_id
""".strip()
    viable_players = int(_run_single_value_query(spark, "PROBE_DEVELOPMENT", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_PROBE_FUTURE_DEVELOPMENT",
            name="Future development pathway has players with longitudinal evidence",
            pillar="Assignment Pathway Readiness",
            category="development",
            status="PASS" if viable_players >= threshold else "FAIL",
            severity="info" if viable_players >= threshold else "error",
            message=(
                f"{viable_players} players have sufficient longitudinal assessment evidence."
                if viable_players >= threshold
                else (
                    f"Only {viable_players} players have sufficient longitudinal assessment evidence; "
                    f"minimum required is {threshold}."
                )
            ),
            affected_count=max(threshold - viable_players, 0),
            observed_value=viable_players,
            expected_value=threshold,
        )
    ]


def _tournament_candidate_probe(
    spark: Any,
    loaded_by_source: dict[str, Any],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    team_source = loaded_by_source.get("teams")
    if team_source is None or team_source.read_status == "UNREADABLE":
        return []

    threshold = int(thresholds["minimum_distinct_candidate_teams_per_country_division"])
    country_filter = _in_list(TARGET_COUNTRIES)
    division_filter = _in_list(TARGET_DIVISIONS)
    query = f"""
WITH valid_pairs AS (
    SELECT
        UPPER(TRIM(CAST(country_code AS STRING))) AS country_code,
        LOWER(TRIM(CAST(team_division AS STRING))) AS division_value,
        COUNT(DISTINCT CAST(id AS STRING)) AS team_count
    FROM {team_source.temp_view_name}
    WHERE id IS NOT NULL
      AND UPPER(TRIM(CAST(team_status AS STRING))) = 'ACTIVE'
      AND UPPER(TRIM(CAST(country_code AS STRING))) IN ({country_filter})
      AND LOWER(TRIM(CAST(team_division AS STRING))) IN ({division_filter})
    GROUP BY
        UPPER(TRIM(CAST(country_code AS STRING))),
        LOWER(TRIM(CAST(team_division AS STRING)))
)
SELECT COUNT(*) AS value
FROM valid_pairs
WHERE team_count < {threshold}
""".strip()
    weak_pairs = int(_run_single_value_query(spark, "PROBE_TOURNAMENT", query))
    return [
        CertificationRuleResult(
            rule_id="RAW_PROBE_TOURNAMENT_CANDIDATES",
            name="Tournament candidate pathway has valid pre-existing team ids",
            pillar="Assignment Pathway Readiness",
            category="tournament",
            status="PASS" if weak_pairs == 0 else "FAIL",
            severity="info" if weak_pairs == 0 else "blocker",
            message=(
                "Tournament candidate probe passed for all target country/division combinations."
                if weak_pairs == 0
                else (
                    f"{weak_pairs} country/division combinations fall below the valid "
                    f"pre-existing team-id threshold of {threshold}."
                )
            ),
            affected_count=weak_pairs,
            expected_value=threshold,
        )
    ]


def _data_quality_learning_probe(
    structural_results: tuple[CertificationRuleResult, ...],
    thresholds: dict[str, Any],
) -> list[CertificationRuleResult]:
    issue_results = [rule for rule in structural_results if rule.status == "FAIL"]
    issue_rows = sum(max(int(rule.affected_count or 0), 0) for rule in issue_results)
    minimum_issue_rows = int(thresholds["minimum_quality_issue_rows_for_learning"])
    maximum_issue_rate = float(thresholds["maximum_quality_issue_rate_for_learning"])
    critical_issue_results = [
        rule for rule in issue_results if rule.severity in {"error", "blocker"}
    ]
    critical_issue_rate = (
        len(critical_issue_results) / max(len(structural_results), 1)
    )

    if issue_rows < minimum_issue_rows:
        status = "FAIL"
        severity = "warning"
        message = (
            f"The release appears too sterile for the data-quality learning pathway; "
            f"observed issue rows={issue_rows}, minimum expected={minimum_issue_rows}."
        )
    elif critical_issue_rate > maximum_issue_rate:
        status = "FAIL"
        severity = "error"
        message = (
            f"The release has too many serious structural defects for the learning pathway; "
            f"critical_issue_rate={critical_issue_rate:.4f}, maximum allowed={maximum_issue_rate:.4f}."
        )
    else:
        status = "PASS"
        severity = "info"
        message = (
            f"The release contains manageable data-quality learning signal "
            f"(issue_rows={issue_rows}, critical_issue_rate={critical_issue_rate:.4f})."
        )

    return [
        CertificationRuleResult(
            rule_id="RAW_PROBE_DATA_QUALITY_LEARNING",
            name="Data-quality learning pathway has manageable nonzero defect signal",
            pillar="Assignment Pathway Readiness",
            category="data_quality",
            status=status,
            severity=severity,
            message=message,
            affected_count=issue_rows,
            observed_value=round(critical_issue_rate, 6),
            expected_value=f"{minimum_issue_rows}..{maximum_issue_rate}",
        )
    ]


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


def _in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)
