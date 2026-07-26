"""Validation helpers for the Silver-to-Gold Phase 12 recommendation harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import get_gold_target_table_fqn
from napa_pipeline.silver_to_gold.recommendations import (
    OlympicTeamRecommendationsPublicationSummary,
    publish_phase12_recommendation_table,
)


PHASE12_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "team_selection_scorecards": (
        "team_id",
        "scoring_scenario",
        "analysis_as_of_date",
        "team_category",
        "country_code",
        "player_one_id",
        "player_two_id",
        "final_team_selection_score",
        "combined_team_confidence",
        "top_strengths",
        "top_risks",
        "ranking_rationale",
        "eligibility_status",
    ),
}


class Phase12SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 12 fields."""


@dataclass(frozen=True)
class Phase12PublishedTables:
    """Published-table summary for the Phase 12 target table."""

    olympic_team_recommendations: OlympicTeamRecommendationsPublicationSummary


def validate_phase12_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed source tables expose required Phase 12 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for table_name, required_columns in PHASE12_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = get_gold_target_table_fqn(environment, table_name)
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase12SourceContractError(
                f"Phase 12 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[table_name] = required_columns
    return validated_columns


def publish_phase12_recommendation_tables(
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
    return publish_phase12_recommendation_table(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        release_name=release_name,
        release_role=release_role,
        authoritative_recommendation_flag=authoritative_recommendation_flag,
        pipeline_version=pipeline_version,
        eligibility_config=eligibility_config,
    )
