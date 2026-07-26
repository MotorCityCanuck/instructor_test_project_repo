"""Phase 12 recommendation builders for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_stage_table_fqn,
    get_gold_target_table_fqn,
)
from napa_pipeline.silver_to_gold.publish import publish_stage_to_gold_table
from napa_pipeline.silver_to_gold.team_selection import ELIGIBLE_STATUS


PRIMARY_STATUS = "PRIMARY"
ALTERNATE_STATUS = "ALTERNATE"
WATCHLIST_STATUS = "WATCHLIST"
RANKED_CANDIDATE_STATUS = "RANKED_CANDIDATE"


@dataclass(frozen=True)
class OlympicTeamRecommendationsPublicationSummary:
    """Published-table summary for olympic_team_recommendations."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


def publish_phase12_recommendation_table(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    pipeline_version: str,
    eligibility_config: dict[str, Any],
) -> OlympicTeamRecommendationsPublicationSummary:
    """Publish the Gold Phase 12 recommendation table."""
    return publish_olympic_team_recommendations_from_sql(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        release_name=release_name,
        release_role=release_role,
        authoritative_recommendation_flag=authoritative_recommendation_flag,
        methodology_version=pipeline_version,
        eligibility_config=eligibility_config,
    )


def build_olympic_team_recommendations(
    *,
    team_selection_scorecard_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    methodology_version: str,
    eligibility_config: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Build one recommendation row per eligible team and scoring scenario."""
    primary_count = int(eligibility_config.get("primary_teams_per_country_category", 1))
    alternate_count = int(eligibility_config.get("alternate_teams_per_country_category", 2))
    watchlist_count = int(eligibility_config.get("watchlist_teams_per_country_category", 3))
    configured_total = primary_count + alternate_count + watchlist_count

    eligible_rows = [
        row
        for row in team_selection_scorecard_rows
        if _normalize_required_string(row.get("scoring_scenario")) == scoring_scenario
        and _coerce_date(row.get("analysis_as_of_date")) == analysis_as_of_date
        and _normalize_required_string(row.get("eligibility_status")) == ELIGIBLE_STATUS
        and _coerce_float(row.get("final_team_selection_score")) is not None
    ]

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in eligible_rows:
        key = (
            _normalize_required_string(row.get("country_code")),
            _normalize_required_string(row.get("team_category")),
        )
        grouped_rows.setdefault(key, []).append(row)

    recommendation_rows: list[dict[str, Any]] = []
    for (country_code, category_code), rows in sorted(grouped_rows.items()):
        ordered_rows = sorted(
            rows,
            key=lambda row: (
                -float(_coerce_float(row.get("final_team_selection_score")) or 0.0),
                -float(_coerce_float(row.get("combined_team_confidence")) or 0.0),
                str(row.get("team_id") or ""),
            ),
        )
        available_candidate_count = len(ordered_rows)
        shortfall_reason = _shortfall_reason(
            available_candidate_count=available_candidate_count,
            configured_total=configured_total,
        )

        for index, row in enumerate(ordered_rows):
            rank = index + 1
            recommendation_status = _recommendation_status(
                rank,
                primary_count=primary_count,
                alternate_count=alternate_count,
                watchlist_count=watchlist_count,
            )
            primary_score = _coerce_float(ordered_rows[0].get("final_team_selection_score"))
            current_score = _coerce_float(row.get("final_team_selection_score"))
            previous_row = ordered_rows[index - 1] if index > 0 else None
            next_row = ordered_rows[index + 1] if index + 1 < available_candidate_count else None
            closest_alternative_row = next_row if rank == 1 else previous_row
            previous_score = (
                _coerce_float(previous_row.get("final_team_selection_score"))
                if previous_row is not None
                else None
            )
            closest_alternative_score = (
                _coerce_float(closest_alternative_row.get("final_team_selection_score"))
                if closest_alternative_row is not None
                else None
            )

            recommendation_rows.append(
                {
                    "country_code": country_code,
                    "category_code": category_code,
                    "team_id": row.get("team_id"),
                    "scoring_scenario": scoring_scenario,
                    "analysis_as_of_date": analysis_as_of_date,
                    "release_name": release_name,
                    "release_role": release_role,
                    "authoritative_recommendation_flag": authoritative_recommendation_flag,
                    "recommendation_status": recommendation_status,
                    "candidate_rank": rank,
                    "player_one_id": row.get("player_one_id"),
                    "player_two_id": row.get("player_two_id"),
                    "final_team_selection_score": current_score,
                    "combined_team_confidence": _coerce_float(
                        row.get("combined_team_confidence")
                    ),
                    "selection_rationale": _selection_rationale(
                        row,
                        recommendation_status=recommendation_status,
                        candidate_rank=rank,
                    ),
                    "alternate_rationale": _alternate_rationale(
                        current_team_id=_normalize_optional_string(row.get("team_id")),
                        current_rank=rank,
                        closest_alternative_team_id=_normalize_optional_string(
                            closest_alternative_row.get("team_id")
                        )
                        if closest_alternative_row is not None
                        else None,
                        closest_alternative_score_gap=_score_gap(
                            current_score,
                            closest_alternative_score,
                            primary_row=(rank == 1),
                        ),
                    ),
                    "key_risks": _normalize_optional_string(row.get("top_risks")),
                    "methodology_version": methodology_version,
                    "score_gap_to_primary": _primary_gap(
                        primary_score=primary_score,
                        current_score=current_score,
                    ),
                    "score_gap_to_previous": (
                        _score_gap(current_score, previous_score, primary_row=False)
                        if rank > 1
                        else None
                    ),
                    "closest_alternative_team_id": _normalize_optional_string(
                        closest_alternative_row.get("team_id")
                    )
                    if closest_alternative_row is not None
                    else None,
                    "closest_alternative_score_gap": _score_gap(
                        current_score,
                        closest_alternative_score,
                        primary_row=(rank == 1),
                    ),
                    "configured_primary_count": primary_count,
                    "configured_alternate_count": alternate_count,
                    "configured_watchlist_count": watchlist_count,
                    "available_candidate_count": available_candidate_count,
                    "recommendation_shortfall_flag": available_candidate_count < configured_total,
                    "recommendation_shortfall_reason": shortfall_reason,
                    "constraint_applied_flag": False,
                    "constraint_reason": None,
                    "unconstrained_rank": rank,
                    "constrained_selection_status": recommendation_status,
                }
            )

    return tuple(recommendation_rows)


def build_olympic_team_recommendations_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    methodology_version: str,
    eligibility_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build olympic_team_recommendations."""
    scorecards_fqn = get_gold_target_table_fqn(environment, "team_selection_scorecards")
    primary_count = int(eligibility_config.get("primary_teams_per_country_category", 1))
    alternate_count = int(eligibility_config.get("alternate_teams_per_country_category", 2))
    watchlist_count = int(eligibility_config.get("watchlist_teams_per_country_category", 3))
    configured_total = primary_count + alternate_count + watchlist_count
    analysis_date_text = analysis_as_of_date.isoformat()
    release_name_text = _sql_string_literal(release_name)
    release_role_text = _sql_string_literal(release_role)
    methodology_version_text = _sql_string_literal(methodology_version)

    return f"""
WITH eligible_scorecards AS (
    SELECT
        country_code,
        team_category AS category_code,
        team_id,
        scoring_scenario,
        analysis_as_of_date,
        player_one_id,
        player_two_id,
        final_team_selection_score,
        combined_team_confidence,
        top_strengths,
        top_risks,
        ranking_rationale
    FROM {scorecards_fqn}
    WHERE scoring_scenario = '{_sql_string_literal(scoring_scenario)}'
      AND analysis_as_of_date = DATE '{analysis_date_text}'
      AND eligibility_status = '{ELIGIBLE_STATUS}'
      AND final_team_selection_score IS NOT NULL
),
ranked_rows AS (
    SELECT
        country_code,
        category_code,
        team_id,
        scoring_scenario,
        analysis_as_of_date,
        ROW_NUMBER() OVER (
            PARTITION BY country_code, category_code, scoring_scenario
            ORDER BY final_team_selection_score DESC,
                     combined_team_confidence DESC,
                     team_id ASC
        ) AS candidate_rank,
        COUNT(*) OVER (
            PARTITION BY country_code, category_code, scoring_scenario
        ) AS available_candidate_count,
        player_one_id,
        player_two_id,
        final_team_selection_score,
        combined_team_confidence,
        top_strengths,
        top_risks,
        ranking_rationale,
        FIRST_VALUE(final_team_selection_score) OVER (
            PARTITION BY country_code, category_code, scoring_scenario
            ORDER BY final_team_selection_score DESC,
                     combined_team_confidence DESC,
                     team_id ASC
        ) AS primary_team_selection_score,
        LAG(team_id) OVER (
            PARTITION BY country_code, category_code, scoring_scenario
            ORDER BY final_team_selection_score DESC,
                     combined_team_confidence DESC,
                     team_id ASC
        ) AS previous_team_id,
        LAG(final_team_selection_score) OVER (
            PARTITION BY country_code, category_code, scoring_scenario
            ORDER BY final_team_selection_score DESC,
                     combined_team_confidence DESC,
                     team_id ASC
        ) AS previous_team_selection_score,
        LEAD(team_id) OVER (
            PARTITION BY country_code, category_code, scoring_scenario
            ORDER BY final_team_selection_score DESC,
                     combined_team_confidence DESC,
                     team_id ASC
        ) AS next_team_id,
        LEAD(final_team_selection_score) OVER (
            PARTITION BY country_code, category_code, scoring_scenario
            ORDER BY final_team_selection_score DESC,
                     combined_team_confidence DESC,
                     team_id ASC
        ) AS next_team_selection_score
    FROM eligible_scorecards
),
classified_rows AS (
    SELECT
        country_code,
        category_code,
        team_id,
        scoring_scenario,
        analysis_as_of_date,
        '{release_name_text}' AS release_name,
        '{release_role_text}' AS release_role,
        CAST({'true' if authoritative_recommendation_flag else 'false'} AS BOOLEAN)
            AS authoritative_recommendation_flag,
        CASE
            WHEN candidate_rank <= {primary_count} THEN '{PRIMARY_STATUS}'
            WHEN candidate_rank <= {primary_count + alternate_count} THEN '{ALTERNATE_STATUS}'
            WHEN candidate_rank <= {primary_count + alternate_count + watchlist_count} THEN '{WATCHLIST_STATUS}'
            ELSE '{RANKED_CANDIDATE_STATUS}'
        END AS recommendation_status,
        candidate_rank,
        player_one_id,
        player_two_id,
        final_team_selection_score,
        combined_team_confidence,
        CONCAT(
            'Status=',
            CASE
                WHEN candidate_rank <= {primary_count} THEN '{PRIMARY_STATUS}'
                WHEN candidate_rank <= {primary_count + alternate_count} THEN '{ALTERNATE_STATUS}'
                WHEN candidate_rank <= {primary_count + alternate_count + watchlist_count} THEN '{WATCHLIST_STATUS}'
                ELSE '{RANKED_CANDIDATE_STATUS}'
            END,
            '; Rank=',
            CAST(candidate_rank AS STRING),
            '; Strengths=',
            COALESCE(top_strengths, 'none'),
            '; Base=',
            COALESCE(ranking_rationale, 'none')
        ) AS selection_rationale,
        CASE
            WHEN candidate_rank = 1 THEN CONCAT(
                'Closest alternate team ',
                COALESCE(next_team_id, 'NA'),
                ' trails by ',
                COALESCE(CAST(ROUND(final_team_selection_score - next_team_selection_score, 4) AS STRING), 'NA')
            )
            ELSE CONCAT(
                'Higher-ranked alternative team ',
                COALESCE(previous_team_id, 'NA'),
                ' leads by ',
                COALESCE(CAST(ROUND(previous_team_selection_score - final_team_selection_score, 4) AS STRING), 'NA')
            )
        END AS alternate_rationale,
        top_risks AS key_risks,
        '{methodology_version_text}' AS methodology_version,
        ROUND(primary_team_selection_score - final_team_selection_score, 4) AS score_gap_to_primary,
        CASE
            WHEN candidate_rank = 1 THEN NULL
            ELSE ROUND(previous_team_selection_score - final_team_selection_score, 4)
        END AS score_gap_to_previous,
        CASE
            WHEN candidate_rank = 1 THEN next_team_id
            ELSE previous_team_id
        END AS closest_alternative_team_id,
        CASE
            WHEN candidate_rank = 1 AND next_team_selection_score IS NOT NULL THEN
                ROUND(final_team_selection_score - next_team_selection_score, 4)
            WHEN candidate_rank > 1 AND previous_team_selection_score IS NOT NULL THEN
                ROUND(previous_team_selection_score - final_team_selection_score, 4)
            ELSE NULL
        END AS closest_alternative_score_gap,
        {primary_count} AS configured_primary_count,
        {alternate_count} AS configured_alternate_count,
        {watchlist_count} AS configured_watchlist_count,
        available_candidate_count,
        CAST(available_candidate_count < {configured_total} AS BOOLEAN) AS recommendation_shortfall_flag,
        CASE
            WHEN available_candidate_count < {configured_total} THEN CONCAT(
                'AVAILABLE_CANDIDATES_BELOW_CONFIGURED_TARGET:',
                CAST(available_candidate_count AS STRING),
                '/',
                CAST({configured_total} AS STRING)
            )
            ELSE NULL
        END AS recommendation_shortfall_reason,
        CAST(false AS BOOLEAN) AS constraint_applied_flag,
        CAST(NULL AS STRING) AS constraint_reason,
        candidate_rank AS unconstrained_rank,
        CASE
            WHEN candidate_rank <= {primary_count} THEN '{PRIMARY_STATUS}'
            WHEN candidate_rank <= {primary_count + alternate_count} THEN '{ALTERNATE_STATUS}'
            WHEN candidate_rank <= {primary_count + alternate_count + watchlist_count} THEN '{WATCHLIST_STATUS}'
            ELSE '{RANKED_CANDIDATE_STATUS}'
        END AS constrained_selection_status
    FROM ranked_rows
)
SELECT
    country_code,
    category_code,
    team_id,
    scoring_scenario,
    analysis_as_of_date,
    release_name,
    release_role,
    authoritative_recommendation_flag,
    recommendation_status,
    candidate_rank,
    player_one_id,
    player_two_id,
    final_team_selection_score,
    combined_team_confidence,
    selection_rationale,
    alternate_rationale,
    key_risks,
    methodology_version,
    score_gap_to_primary,
    score_gap_to_previous,
    closest_alternative_team_id,
    closest_alternative_score_gap,
    configured_primary_count,
    configured_alternate_count,
    configured_watchlist_count,
    available_candidate_count,
    recommendation_shortfall_flag,
    recommendation_shortfall_reason,
    constraint_applied_flag,
    constraint_reason,
    unconstrained_rank,
    constrained_selection_status
FROM classified_rows
""".strip()


def publish_olympic_team_recommendations_from_sql(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    methodology_version: str,
    eligibility_config: dict[str, Any],
) -> OlympicTeamRecommendationsPublicationSummary:
    """Build and publish olympic_team_recommendations using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "olympic_team_recommendations")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "olympic_team_recommendations")
    stage_row_count, output_row_count = publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_olympic_team_recommendations_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
            scoring_scenario=scoring_scenario,
            release_name=release_name,
            release_role=release_role,
            authoritative_recommendation_flag=authoritative_recommendation_flag,
            methodology_version=methodology_version,
            eligibility_config=eligibility_config,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("country_code", "category_code", "team_id", "scoring_scenario"),
            label="olympic_team_recommendations",
        ),
    )
    return OlympicTeamRecommendationsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=stage_row_count,
        output_row_count=output_row_count,
    )


def _recommendation_status(
    rank: int,
    *,
    primary_count: int,
    alternate_count: int,
    watchlist_count: int,
) -> str:
    if rank <= primary_count:
        return PRIMARY_STATUS
    if rank <= primary_count + alternate_count:
        return ALTERNATE_STATUS
    if rank <= primary_count + alternate_count + watchlist_count:
        return WATCHLIST_STATUS
    return RANKED_CANDIDATE_STATUS


def _selection_rationale(
    row: dict[str, Any],
    *,
    recommendation_status: str,
    candidate_rank: int,
) -> str:
    strengths = _normalize_optional_string(row.get("top_strengths")) or "none"
    base_rationale = _normalize_optional_string(row.get("ranking_rationale")) or "none"
    return (
        f"Status={recommendation_status}; Rank={candidate_rank}; "
        f"Strengths={strengths}; Base={base_rationale}"
    )


def _alternate_rationale(
    *,
    current_team_id: str | None,
    current_rank: int,
    closest_alternative_team_id: str | None,
    closest_alternative_score_gap: float | None,
) -> str:
    if current_rank == 1:
        return (
            "Closest alternate team "
            f"{closest_alternative_team_id or 'NA'} trails by "
            f"{closest_alternative_score_gap if closest_alternative_score_gap is not None else 'NA'}"
        )
    return (
        "Higher-ranked alternative team "
        f"{closest_alternative_team_id or 'NA'} leads "
        f"{current_team_id or 'NA'} by "
        f"{closest_alternative_score_gap if closest_alternative_score_gap is not None else 'NA'}"
    )


def _primary_gap(*, primary_score: float | None, current_score: float | None) -> float | None:
    if primary_score is None or current_score is None:
        return None
    return round(primary_score - current_score, 4)


def _score_gap(
    current_score: float | None,
    comparison_score: float | None,
    *,
    primary_row: bool,
) -> float | None:
    if current_score is None or comparison_score is None:
        return None
    if primary_row:
        return round(current_score - comparison_score, 4)
    return round(comparison_score - current_score, 4)


def _shortfall_reason(*, available_candidate_count: int, configured_total: int) -> str | None:
    if available_candidate_count >= configured_total:
        return None
    return (
        "AVAILABLE_CANDIDATES_BELOW_CONFIGURED_TARGET:"
        f"{available_candidate_count}/{configured_total}"
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
    COALESCE(SUM(CASE WHEN {null_conditions} THEN 1 ELSE 0 END), 0) AS null_key_count,
    COALESCE(SUM(CASE WHEN duplicate_key_count > 1 THEN 1 ELSE 0 END), 0) AS duplicate_group_count
FROM (
    SELECT
        {grouping},
        COUNT(*) AS duplicate_key_count
    FROM {table_fqn}
    GROUP BY {grouping}
)
""".strip()
    ).collect()[0]
    mapping = (
        validation_row.asDict(recursive=True)
        if hasattr(validation_row, "asDict")
        else dict(validation_row)
    )
    if int(mapping["null_key_count"] or 0) != 0 or int(mapping["duplicate_group_count"] or 0) != 0:
        raise ValueError(
            f"{label} validation failed for {table_fqn}: "
            f"null_key_count={int(mapping['null_key_count'] or 0)}, "
            f"duplicate_group_count={int(mapping['duplicate_group_count'] or 0)}."
        )


def _normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_required_string(value: Any) -> str:
    normalized = _normalize_optional_string(value)
    if normalized is None:
        raise ValueError("Expected a non-empty string value.")
    return normalized


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _coerce_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _sql_string_literal(value: str) -> str:
    return str(value).replace("'", "''")
