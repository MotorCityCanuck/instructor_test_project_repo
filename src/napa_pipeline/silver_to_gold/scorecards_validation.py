"""Validation helpers for the Silver-to-Gold Phase 10 player scorecard harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.scorecards import (
    NationalPlayerRankingsPublicationSummary,
    Phase10PublicationSummary,
    PlayerEvaluationScorecardsPublicationSummary,
    publish_phase10_player_tables,
)


PHASE10_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "players": (
        "silver",
        "players",
        (
            "player_id",
            "display_name",
            "country_code",
            "gender",
            "active_flag",
        ),
    ),
    "player_current_ratings": (
        "gold",
        "player_current_ratings",
        (
            "player_id",
            "source_rating_value",
            "source_confidence_score",
            "analytical_rating_value",
            "rated_match_count",
            "rating_reliability_score",
            "rating_evidence_band",
            "rating_uncertainty_proxy",
        ),
    ),
    "player_performance_features": (
        "gold",
        "player_performance_features",
        (
            "player_id",
            "evidence_window",
            "match_count",
            "performance_above_expectation",
            "game_win_pct",
            "win_pct",
            "recency_weighted_win_pct",
            "consistency_score",
            "strength_of_schedule",
        ),
    ),
    "player_development_features": (
        "gold",
        "player_development_features",
        (
            "player_id",
            "latest_assessment_confidence",
            "development_momentum_score",
            "feature_evidence_status",
        ),
    ),
    "entity_data_quality_confidence": (
        "gold",
        "entity_data_quality_confidence",
        (
            "entity_type",
            "entity_id",
            "data_quality_confidence_score",
            "quality_confidence_band",
            "material_limitation_text",
        ),
    ),
}


class Phase10SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 10 fields."""


@dataclass(frozen=True)
class Phase10PublishedTables:
    """Published-table summary for the Phase 10 target tables."""

    player_evaluation_scorecards: PlayerEvaluationScorecardsPublicationSummary
    national_player_rankings: NationalPlayerRankingsPublicationSummary


def validate_phase10_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed source tables expose required Phase 10 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for logical_name, (layer, table_name, required_columns) in PHASE10_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = (
            get_gold_target_table_fqn(environment, table_name)
            if layer == "gold"
            else get_silver_source_table_fqn(environment, table_name)
        )
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase10SourceContractError(
                f"Phase 10 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[logical_name] = required_columns
    return validated_columns


def publish_phase10_scorecard_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
) -> Phase10PublicationSummary:
    """Publish the Gold Phase 10 player scorecard and ranking tables."""
    return publish_phase10_player_tables(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        scorecards_config=scorecards_config,
        eligibility_config=eligibility_config,
    )
