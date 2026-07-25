"""Validation helpers for the Silver-to-Gold Phase 6 feature harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.features import (
    PlayerDevelopmentFeaturesPublicationSummary,
    PlayerPerformanceFeaturesPublicationSummary,
    publish_player_development_features,
    publish_player_performance_features,
)
from napa_pipeline.silver_to_gold.io import (
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)


PHASE6_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "competition_player_matches": (
        "gold",
        "competition_player_matches",
        (
            "match_id",
            "match_date",
            "batch_sequence",
            "player_id",
            "partner_player_id",
            "won_flag",
            "lost_flag",
            "games_won",
            "games_lost",
            "point_share",
            "point_differential",
            "pre_match_team_rating",
            "pre_match_opponent_team_rating",
        ),
    ),
    "player_current_ratings": (
        "gold",
        "player_current_ratings",
        (
            "player_id",
            "analytical_rating_value",
            "rated_match_count",
        ),
    ),
    "player_rating_history": (
        "gold",
        "player_rating_history",
        (
            "player_id",
            "rating_effective_date",
            "analytical_rating_value",
            "rated_match_count",
        ),
    ),
    "players": (
        "silver",
        "players",
        (
            "player_id",
            "display_name",
            "country_code",
            "active_flag",
        ),
    ),
    "player_assessment_history": (
        "silver",
        "player_assessment_history",
        (
            "player_id",
            "assessment_date",
            "assessment_value",
            "assessment_confidence",
        ),
    ),
    "player_registrations": (
        "silver",
        "player_registrations",
        (
            "player_id",
            "registration_date",
            "current_registration_flag",
        ),
    ),
}


class Phase6SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 6 fields."""


@dataclass(frozen=True)
class Phase6PublicationSummary:
    """Published-table summaries for the two Phase 6 target tables."""

    player_performance_features: PlayerPerformanceFeaturesPublicationSummary
    player_development_features: PlayerDevelopmentFeaturesPublicationSummary


def validate_phase6_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed source tables expose the required Phase 6 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for logical_name, (layer, table_name, required_columns) in PHASE6_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = (
            get_gold_target_table_fqn(environment, table_name)
            if layer == "gold"
            else get_silver_source_table_fqn(environment, table_name)
        )
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase6SourceContractError(
                f"Phase 6 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[logical_name] = required_columns
    return validated_columns


def publish_phase6_feature_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    features_config: dict[str, Any],
    evidence_windows_config: dict[str, Any],
) -> Phase6PublicationSummary:
    """Publish the two Gold Phase 6 feature tables."""
    performance_summary = publish_player_performance_features(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        features_config=features_config,
        evidence_windows_config=evidence_windows_config,
    )
    development_summary = publish_player_development_features(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        features_config=features_config,
        evidence_windows_config=evidence_windows_config,
    )
    return Phase6PublicationSummary(
        player_performance_features=performance_summary,
        player_development_features=development_summary,
    )
