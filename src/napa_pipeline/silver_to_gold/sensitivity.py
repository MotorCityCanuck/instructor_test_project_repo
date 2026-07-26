"""Phase 13 sensitivity and explainability builders for the Silver-to-Gold pipeline."""

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


PRIMARY_STATUS = "PRIMARY"
ALTERNATE_STATUS = "ALTERNATE"
WATCHLIST_STATUS = "WATCHLIST"
RANKED_CANDIDATE_STATUS = "RANKED_CANDIDATE"
PENDING_REVIEW_STATUS = "PENDING_REVIEW"


@dataclass(frozen=True)
class SelectionSensitivityResultsPublicationSummary:
    """Published-table summary for selection_sensitivity_results."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class RecommendationExplanationsPublicationSummary:
    """Published-table summary for recommendation_explanations."""

    target_table_fqn: str
    stage_table_fqn: str
    input_row_count: int
    output_row_count: int


@dataclass(frozen=True)
class Phase13PublicationSummary:
    """Published-table summary for the two Phase 13 target tables."""

    selection_sensitivity_results: SelectionSensitivityResultsPublicationSummary
    recommendation_explanations: RecommendationExplanationsPublicationSummary


def publish_phase13_sensitivity_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    pipeline_version: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
    sensitivity_config: dict[str, Any],
) -> Phase13PublicationSummary:
    """Publish the Gold Phase 13 sensitivity and explanation tables."""
    sensitivity_summary = publish_selection_sensitivity_results_from_sql(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        release_name=release_name,
        release_role=release_role,
        authoritative_recommendation_flag=authoritative_recommendation_flag,
        methodology_version=pipeline_version,
        scorecards_config=scorecards_config,
        eligibility_config=eligibility_config,
        sensitivity_config=sensitivity_config,
    )
    explanation_summary = publish_recommendation_explanations_from_sql(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        release_name=release_name,
        release_role=release_role,
        authoritative_recommendation_flag=authoritative_recommendation_flag,
        methodology_version=pipeline_version,
    )
    return Phase13PublicationSummary(
        selection_sensitivity_results=sensitivity_summary,
        recommendation_explanations=explanation_summary,
    )


def build_selection_sensitivity_results_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    methodology_version: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
    sensitivity_config: dict[str, Any],
) -> str:
    """Return the Spark SQL used to build selection_sensitivity_results."""
    scorecards_fqn = get_gold_target_table_fqn(environment, "team_selection_scorecards")
    recommendations_fqn = get_gold_target_table_fqn(environment, "olympic_team_recommendations")
    analysis_date_text = analysis_as_of_date.isoformat()
    primary_count = int(eligibility_config.get("primary_teams_per_country_category", 1))
    alternate_count = int(eligibility_config.get("alternate_teams_per_country_category", 2))
    watchlist_count = int(eligibility_config.get("watchlist_teams_per_country_category", 3))
    scenario_rows = _scenario_rows_sql(
        scorecards_config=scorecards_config,
        sensitivity_config=sensitivity_config,
    )

    return f"""
WITH scenario_definitions AS (
    {scenario_rows}
),
candidate_universe AS (
    SELECT
        recommendations.country_code,
        recommendations.category_code,
        recommendations.team_id,
        recommendations.scoring_scenario AS baseline_scoring_scenario,
        recommendations.analysis_as_of_date,
        recommendations.release_name,
        recommendations.release_role,
        recommendations.authoritative_recommendation_flag,
        recommendations.recommendation_status AS baseline_recommendation_status,
        scorecards.player_one_id,
        scorecards.player_two_id,
        scorecards.partnership_score,
        scorecards.player_strength_score,
        scorecards.prediction_score,
        scorecards.confidence_component_score,
        scorecards.confidence_factor,
        scorecards.risk_penalty_score,
        scorecards.top_strengths,
        scorecards.top_risks,
        scorecards.material_limitation_text,
        scorecards.current_member_count,
        scorecards.evidence_sufficiency_status,
        scorecards.combined_team_confidence
    FROM {recommendations_fqn} AS recommendations
    INNER JOIN {scorecards_fqn} AS scorecards
        ON recommendations.team_id = scorecards.team_id
       AND recommendations.scoring_scenario = scorecards.scoring_scenario
       AND recommendations.analysis_as_of_date = scorecards.analysis_as_of_date
    WHERE recommendations.scoring_scenario = '{_sql_string_literal(scoring_scenario)}'
      AND recommendations.analysis_as_of_date = DATE '{analysis_date_text}'
),
scenario_scores AS (
    SELECT
        candidates.country_code,
        candidates.category_code,
        candidates.team_id,
        candidates.baseline_scoring_scenario,
        scenarios.scenario_name,
        candidates.analysis_as_of_date,
        candidates.release_name,
        candidates.release_role,
        candidates.authoritative_recommendation_flag,
        candidates.baseline_recommendation_status,
        ROUND(
            (
                (
                    COALESCE(candidates.partnership_score, 0.0) * scenarios.partnership_weight
                    + COALESCE(candidates.player_strength_score, 0.0) * scenarios.player_strength_weight
                    + COALESCE(candidates.prediction_score, 0.0) * scenarios.prediction_weight
                    + COALESCE(candidates.confidence_component_score, 0.0) * scenarios.confidence_weight
                ) / NULLIF(
                    (CASE WHEN candidates.partnership_score IS NOT NULL THEN scenarios.partnership_weight ELSE 0.0 END)
                    + (CASE WHEN candidates.player_strength_score IS NOT NULL THEN scenarios.player_strength_weight ELSE 0.0 END)
                    + (CASE WHEN candidates.prediction_score IS NOT NULL THEN scenarios.prediction_weight ELSE 0.0 END)
                    + (CASE WHEN candidates.confidence_component_score IS NOT NULL THEN scenarios.confidence_weight ELSE 0.0 END),
                    0.0
                )
            ),
            4
        ) AS scenario_raw_score,
        ROUND(
            (
                ROUND(
                    (
                        (
                            COALESCE(candidates.partnership_score, 0.0) * scenarios.partnership_weight
                            + COALESCE(candidates.player_strength_score, 0.0) * scenarios.player_strength_weight
                            + COALESCE(candidates.prediction_score, 0.0) * scenarios.prediction_weight
                            + COALESCE(candidates.confidence_component_score, 0.0) * scenarios.confidence_weight
                        ) / NULLIF(
                            (CASE WHEN candidates.partnership_score IS NOT NULL THEN scenarios.partnership_weight ELSE 0.0 END)
                            + (CASE WHEN candidates.player_strength_score IS NOT NULL THEN scenarios.player_strength_weight ELSE 0.0 END)
                            + (CASE WHEN candidates.prediction_score IS NOT NULL THEN scenarios.prediction_weight ELSE 0.0 END)
                            + (CASE WHEN candidates.confidence_component_score IS NOT NULL THEN scenarios.confidence_weight ELSE 0.0 END),
                            0.0
                        )
                    ),
                    4
                ) * COALESCE(candidates.confidence_factor, 1.0)
            ) - COALESCE(candidates.risk_penalty_score, 0.0),
            4
        ) AS scenario_score,
        candidates.player_one_id,
        candidates.player_two_id,
        candidates.top_strengths,
        candidates.top_risks,
        candidates.material_limitation_text,
        candidates.current_member_count,
        candidates.evidence_sufficiency_status,
        candidates.combined_team_confidence
    FROM candidate_universe AS candidates
    CROSS JOIN scenario_definitions AS scenarios
),
ranked_rows AS (
    SELECT
        country_code,
        category_code,
        team_id,
        baseline_scoring_scenario AS scoring_scenario,
        scenario_name,
        analysis_as_of_date,
        release_name,
        release_role,
        authoritative_recommendation_flag,
        baseline_recommendation_status,
        ROW_NUMBER() OVER (
            PARTITION BY country_code, category_code, scenario_name
            ORDER BY scenario_score DESC,
                     combined_team_confidence DESC,
                     team_id ASC
        ) AS scenario_rank,
        scenario_score,
        player_one_id,
        player_two_id,
        top_strengths,
        top_risks,
        material_limitation_text,
        current_member_count,
        evidence_sufficiency_status,
        combined_team_confidence,
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY country_code, category_code, scenario_name
                ORDER BY scenario_score DESC,
                         combined_team_confidence DESC,
                         team_id ASC
            ) <= {primary_count}
                THEN TRUE
            ELSE FALSE
        END AS primary_selection_flag,
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY country_code, category_code, scenario_name
                ORDER BY scenario_score DESC,
                         combined_team_confidence DESC,
                         team_id ASC
            ) <= {primary_count}
                THEN '{PRIMARY_STATUS}'
            WHEN ROW_NUMBER() OVER (
                PARTITION BY country_code, category_code, scenario_name
                ORDER BY scenario_score DESC,
                         combined_team_confidence DESC,
                         team_id ASC
            ) <= {primary_count + alternate_count}
                THEN '{ALTERNATE_STATUS}'
            WHEN ROW_NUMBER() OVER (
                PARTITION BY country_code, category_code, scenario_name
                ORDER BY scenario_score DESC,
                         combined_team_confidence DESC,
                         team_id ASC
            ) <= {primary_count + alternate_count + watchlist_count}
                THEN '{WATCHLIST_STATUS}'
            ELSE '{RANKED_CANDIDATE_STATUS}'
        END AS scenario_recommendation_status
    FROM scenario_scores
),
candidate_stats AS (
    SELECT
        country_code,
        category_code,
        team_id,
        MIN(scenario_rank) AS best_scenario_rank,
        MAX(scenario_rank) AS worst_scenario_rank,
        MAX(scenario_rank) - MIN(scenario_rank) AS rank_range_across_scenarios,
        ROUND(AVG(CASE WHEN primary_selection_flag THEN 1.0 ELSE 0.0 END), 4) AS selection_frequency,
        ROUND(
            (
                AVG(CASE WHEN primary_selection_flag THEN 1.0 ELSE 0.0 END) * 100.0
                + (
                    1.0 - (
                        CAST(MAX(scenario_rank) - MIN(scenario_rank) AS DOUBLE)
                        / NULLIF(CAST(COUNT(*) - 1 AS DOUBLE), 0.0)
                    )
                ) * 100.0
            ) / 2.0,
            4
        ) AS recommendation_stability_score
    FROM ranked_rows
    GROUP BY country_code, category_code, team_id
)
SELECT
    ranked.country_code,
    ranked.category_code,
    ranked.team_id,
    ranked.scoring_scenario,
    ranked.scenario_name,
    ranked.analysis_as_of_date,
    ranked.release_name,
    ranked.release_role,
    ranked.authoritative_recommendation_flag,
    ranked.baseline_recommendation_status,
    ranked.scenario_recommendation_status,
    ranked.scenario_rank,
    ranked.scenario_score,
    ranked.primary_selection_flag,
    stats.best_scenario_rank,
    stats.worst_scenario_rank,
    stats.rank_range_across_scenarios,
    stats.selection_frequency,
    stats.recommendation_stability_score,
    ranked.player_one_id,
    ranked.player_two_id,
    ranked.top_strengths,
    ranked.top_risks,
    ranked.material_limitation_text,
    ranked.current_member_count,
    ranked.evidence_sufficiency_status,
    ranked.combined_team_confidence,
    '{_sql_string_literal(methodology_version)}' AS methodology_version
FROM ranked_rows AS ranked
INNER JOIN candidate_stats AS stats
    ON ranked.country_code = stats.country_code
   AND ranked.category_code = stats.category_code
   AND ranked.team_id = stats.team_id
""".strip()


def build_recommendation_explanations_sql(
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    methodology_version: str,
) -> str:
    """Return the Spark SQL used to build recommendation_explanations."""
    scorecards_fqn = get_gold_target_table_fqn(environment, "team_selection_scorecards")
    recommendations_fqn = get_gold_target_table_fqn(environment, "olympic_team_recommendations")
    sensitivity_fqn = get_gold_target_table_fqn(environment, "selection_sensitivity_results")
    analysis_date_text = analysis_as_of_date.isoformat()

    return f"""
WITH sensitivity_stats AS (
    SELECT
        country_code,
        category_code,
        team_id,
        MAX(rank_range_across_scenarios) AS rank_range_across_scenarios,
        MAX(selection_frequency) AS selection_frequency,
        MAX(recommendation_stability_score) AS recommendation_stability_score
    FROM {sensitivity_fqn}
    WHERE scoring_scenario = '{_sql_string_literal(scoring_scenario)}'
      AND analysis_as_of_date = DATE '{analysis_date_text}'
    GROUP BY country_code, category_code, team_id
),
base_rows AS (
    SELECT
        recommendations.country_code,
        recommendations.category_code,
        recommendations.team_id,
        recommendations.scoring_scenario,
        recommendations.analysis_as_of_date,
        recommendations.release_name,
        recommendations.release_role,
        recommendations.authoritative_recommendation_flag,
        recommendations.recommendation_status AS analytical_recommendation_status,
        recommendations.player_one_id,
        recommendations.player_two_id,
        recommendations.final_team_selection_score,
        recommendations.combined_team_confidence,
        recommendations.closest_alternative_team_id,
        recommendations.closest_alternative_score_gap,
        recommendations.key_risks,
        recommendations.methodology_version,
        scorecards.top_strengths,
        scorecards.material_limitation_text,
        scorecards.current_member_count,
        scorecards.evidence_sufficiency_status,
        scorecards.partnership_score,
        scorecards.player_strength_score,
        scorecards.prediction_score,
        scorecards.confidence_component_score,
        sensitivity.rank_range_across_scenarios,
        sensitivity.selection_frequency,
        sensitivity.recommendation_stability_score
    FROM {recommendations_fqn} AS recommendations
    INNER JOIN {scorecards_fqn} AS scorecards
        ON recommendations.team_id = scorecards.team_id
       AND recommendations.scoring_scenario = scorecards.scoring_scenario
       AND recommendations.analysis_as_of_date = scorecards.analysis_as_of_date
    LEFT JOIN sensitivity_stats AS sensitivity
        ON recommendations.country_code = sensitivity.country_code
       AND recommendations.category_code = sensitivity.category_code
       AND recommendations.team_id = sensitivity.team_id
    WHERE recommendations.scoring_scenario = '{_sql_string_literal(scoring_scenario)}'
      AND recommendations.analysis_as_of_date = DATE '{analysis_date_text}'
),
component_arrays AS (
    SELECT
        *,
        array_sort(
            filter(
                array(
                    named_struct('score', partnership_score, 'label', 'partnership'),
                    named_struct('score', player_strength_score, 'label', 'player_strength'),
                    named_struct('score', prediction_score, 'label', 'prediction'),
                    named_struct('score', confidence_component_score, 'label', 'confidence')
                ),
                component -> component.score IS NOT NULL
            )
        ) AS ordered_components
    FROM base_rows
),
component_text AS (
    SELECT
        *,
        CONCAT_WS(
            ',',
            get(reverse(ordered_components), 0).label,
            get(reverse(ordered_components), 1).label,
            get(reverse(ordered_components), 2).label
        ) AS strongest_components,
        CONCAT_WS(
            ',',
            get(ordered_components, 0).label,
            get(ordered_components, 1).label
        ) AS weakest_components
    FROM component_arrays
)
SELECT
    country_code,
    category_code,
    team_id,
    scoring_scenario,
    analysis_as_of_date,
    '{_sql_string_literal(release_name)}' AS release_name,
    '{_sql_string_literal(release_role)}' AS release_role,
    CAST({'true' if authoritative_recommendation_flag else 'false'} AS BOOLEAN)
        AS authoritative_recommendation_flag,
    analytical_recommendation_status,
    '{PENDING_REVIEW_STATUS}' AS human_review_status,
    CAST(false AS BOOLEAN) AS human_override_flag,
    CAST(NULL AS STRING) AS human_override_reason,
    CONCAT(
        'Recommended because the team ranks ',
        CASE
            WHEN analytical_recommendation_status = '{PRIMARY_STATUS}' THEN 'at the top'
            WHEN analytical_recommendation_status = '{ALTERNATE_STATUS}' THEN 'as a strong alternate'
            WHEN analytical_recommendation_status = '{WATCHLIST_STATUS}' THEN 'as a watchlist option'
            ELSE 'within the retained candidate pool'
        END,
        ' with strongest components in ',
        COALESCE(strongest_components, COALESCE(top_strengths, 'available evidence')),
        '.'
    ) AS headline_rationale,
    COALESCE(strongest_components, top_strengths) AS strongest_components,
    weakest_components AS material_weaknesses,
    CONCAT(
        'Members=',
        CAST(COALESCE(current_member_count, 0) AS STRING),
        '; Evidence=',
        COALESCE(evidence_sufficiency_status, 'UNKNOWN')
    ) AS evidence_volume_summary,
    CASE
        WHEN COALESCE(combined_team_confidence, 0.0) >= 80.0 THEN 'HIGH'
        WHEN COALESCE(combined_team_confidence, 0.0) >= 60.0 THEN 'MODERATE'
        ELSE 'LOW'
    END AS confidence_band,
    material_limitation_text AS key_data_limitations,
    closest_alternative_team_id,
    closest_alternative_score_gap,
    selection_frequency,
    recommendation_stability_score,
    rank_range_across_scenarios,
    CONCAT(
        'Selected as PRIMARY in ',
        COALESCE(CAST(ROUND(COALESCE(selection_frequency, 0.0) * 100.0, 2) AS STRING), '0.0'),
        '% of scenarios with rank range ',
        COALESCE(CAST(rank_range_across_scenarios AS STRING), 'NA'),
        '.'
    ) AS sensitivity_summary,
    CONCAT(
        'Headline: ',
        'Recommended because the team ranks ',
        CASE
            WHEN analytical_recommendation_status = '{PRIMARY_STATUS}' THEN 'at the top'
            WHEN analytical_recommendation_status = '{ALTERNATE_STATUS}' THEN 'as a strong alternate'
            WHEN analytical_recommendation_status = '{WATCHLIST_STATUS}' THEN 'as a watchlist option'
            ELSE 'within the retained candidate pool'
        END,
        '. Strengths: ',
        COALESCE(strongest_components, COALESCE(top_strengths, 'available evidence')),
        '. Weaknesses: ',
        COALESCE(weakest_components, 'limited evidence'),
        '. Closest alternative: ',
        COALESCE(closest_alternative_team_id, 'NA'),
        ' (gap ',
        COALESCE(CAST(closest_alternative_score_gap AS STRING), 'NA'),
        '). Stability: ',
        COALESCE(CAST(ROUND(COALESCE(recommendation_stability_score, 0.0), 2) AS STRING), '0.0'),
        '.'
    ) AS explanation_text,
    '{_sql_string_literal(methodology_version)}' AS methodology_version
FROM component_text
""".strip()


def publish_selection_sensitivity_results_from_sql(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    methodology_version: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
    sensitivity_config: dict[str, Any],
) -> SelectionSensitivityResultsPublicationSummary:
    """Build and publish selection_sensitivity_results using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "selection_sensitivity_results")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "selection_sensitivity_results")
    stage_row_count, output_row_count = publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_selection_sensitivity_results_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
            scoring_scenario=scoring_scenario,
            release_name=release_name,
            release_role=release_role,
            authoritative_recommendation_flag=authoritative_recommendation_flag,
            methodology_version=methodology_version,
            scorecards_config=scorecards_config,
            eligibility_config=eligibility_config,
            sensitivity_config=sensitivity_config,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("country_code", "category_code", "team_id", "scenario_name"),
            label="selection_sensitivity_results",
        ),
    )
    return SelectionSensitivityResultsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=stage_row_count,
        output_row_count=output_row_count,
    )


def publish_recommendation_explanations_from_sql(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    release_name: str,
    release_role: str,
    authoritative_recommendation_flag: bool,
    methodology_version: str,
) -> RecommendationExplanationsPublicationSummary:
    """Build and publish recommendation_explanations using Spark-native SQL."""
    target_table_fqn = get_gold_target_table_fqn(environment, "recommendation_explanations")
    stage_table_fqn = get_gold_stage_table_fqn(environment, "recommendation_explanations")
    stage_row_count, output_row_count = publish_stage_to_gold_table(
        spark,
        stage_table_fqn=stage_table_fqn,
        target_table_fqn=target_table_fqn,
        stage_sql=build_recommendation_explanations_sql(
            environment,
            analysis_as_of_date=analysis_as_of_date,
            scoring_scenario=scoring_scenario,
            release_name=release_name,
            release_role=release_role,
            authoritative_recommendation_flag=authoritative_recommendation_flag,
            methodology_version=methodology_version,
        ),
        validation_fn=lambda current_spark, table_fqn: _validate_key_constraints(
            current_spark,
            table_fqn,
            key_columns=("country_code", "category_code", "team_id", "scoring_scenario"),
            label="recommendation_explanations",
        ),
    )
    return RecommendationExplanationsPublicationSummary(
        target_table_fqn=target_table_fqn,
        stage_table_fqn=stage_table_fqn,
        input_row_count=stage_row_count,
        output_row_count=output_row_count,
    )


def _scenario_rows_sql(
    *,
    scorecards_config: dict[str, Any],
    sensitivity_config: dict[str, Any],
) -> str:
    base_weights = {
        str(component): float(weight)
        for component, weight in scorecards_config["team_weights"].items()
    }
    scenario_rows: list[str] = []
    for scenario_name in sensitivity_config["scenarios"]:
        weights = _scenario_weights(str(scenario_name), base_weights=base_weights)
        scenario_rows.append(
            "SELECT "
            f"'{_sql_string_literal(str(scenario_name))}' AS scenario_name, "
            f"{weights['partnership']:.6f} AS partnership_weight, "
            f"{weights['player_strength']:.6f} AS player_strength_weight, "
            f"{weights['prediction']:.6f} AS prediction_weight, "
            f"{weights['confidence']:.6f} AS confidence_weight"
        )
    return "\nUNION ALL\n".join(scenario_rows)


def _scenario_weights(
    scenario_name: str,
    *,
    base_weights: dict[str, float],
) -> dict[str, float]:
    if scenario_name == "BALANCED":
        return dict(base_weights)
    if scenario_name == "PERFORMANCE_HEAVY":
        return {
            "partnership": 0.45,
            "player_strength": 0.20,
            "prediction": 0.25,
            "confidence": 0.10,
        }
    if scenario_name == "RATING_HEAVY":
        return {
            "partnership": 0.15,
            "player_strength": 0.50,
            "prediction": 0.20,
            "confidence": 0.15,
        }
    if scenario_name == "RECENT_FORM_HEAVY":
        return {
            "partnership": 0.20,
            "player_strength": 0.15,
            "prediction": 0.50,
            "confidence": 0.15,
        }
    if scenario_name == "CONFIDENCE_CONSERVATIVE":
        return {
            "partnership": 0.15,
            "player_strength": 0.15,
            "prediction": 0.20,
            "confidence": 0.50,
        }
    if scenario_name == "DEVELOPMENT_ORIENTED":
        return {
            "partnership": 0.15,
            "player_strength": 0.45,
            "prediction": 0.10,
            "confidence": 0.30,
        }
    raise ValueError(f"Unsupported sensitivity scenario: {scenario_name}")


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


def _sql_string_literal(value: str) -> str:
    return str(value).replace("'", "''")
