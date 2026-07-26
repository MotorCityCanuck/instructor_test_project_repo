"""Validation helpers for the Silver-to-Gold Phase 13 sensitivity harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import get_gold_target_table_fqn
from napa_pipeline.silver_to_gold.sensitivity import (
    Phase13PublicationSummary,
    RecommendationExplanationsPublicationSummary,
    SelectionSensitivityResultsPublicationSummary,
    publish_phase13_sensitivity_tables,
)


PHASE13_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "team_selection_scorecards": (
        "team_id",
        "scoring_scenario",
        "analysis_as_of_date",
        "country_code",
        "team_category",
        "player_one_id",
        "player_two_id",
        "partnership_score",
        "player_strength_score",
        "prediction_score",
        "confidence_component_score",
        "confidence_factor",
        "risk_penalty_score",
        "combined_team_confidence",
        "top_strengths",
        "top_risks",
        "material_limitation_text",
        "current_member_count",
        "evidence_sufficiency_status",
    ),
    "olympic_team_recommendations": (
        "country_code",
        "category_code",
        "team_id",
        "scoring_scenario",
        "analysis_as_of_date",
        "release_name",
        "release_role",
        "authoritative_recommendation_flag",
        "recommendation_status",
        "closest_alternative_team_id",
        "closest_alternative_score_gap",
        "methodology_version",
    ),
}


class Phase13SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 13 fields."""


@dataclass(frozen=True)
class Phase13PublishedTables:
    """Published-table summary for the Phase 13 target tables."""

    selection_sensitivity_results: SelectionSensitivityResultsPublicationSummary
    recommendation_explanations: RecommendationExplanationsPublicationSummary


def validate_phase13_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed source tables expose required Phase 13 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for table_name, required_columns in PHASE13_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = get_gold_target_table_fqn(environment, table_name)
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase13SourceContractError(
                f"Phase 13 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[table_name] = required_columns
    return validated_columns


def publish_phase13_sensitivity_products(
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
    return publish_phase13_sensitivity_tables(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        release_name=release_name,
        release_role=release_role,
        authoritative_recommendation_flag=authoritative_recommendation_flag,
        pipeline_version=pipeline_version,
        scorecards_config=scorecards_config,
        eligibility_config=eligibility_config,
        sensitivity_config=sensitivity_config,
    )
