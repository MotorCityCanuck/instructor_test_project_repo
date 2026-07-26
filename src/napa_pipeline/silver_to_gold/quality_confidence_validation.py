"""Validation helpers for the Silver-to-Gold Phase 8 quality-confidence harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.quality_confidence import (
    EntityDataQualityConfidencePublicationSummary,
    publish_entity_data_quality_confidence,
)


PHASE8_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, str, tuple[str, ...]]] = {
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
    "player_performance_features": (
        "gold",
        "player_performance_features",
        (
            "player_id",
            "evidence_window",
            "match_count",
        ),
    ),
    "player_development_features": (
        "gold",
        "player_development_features",
        (
            "player_id",
            "current_registration_flag",
            "latest_assessment_confidence",
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
    "competition_player_matches": (
        "gold",
        "competition_player_matches",
        (
            "player_id",
            "match_id",
            "match_team_id",
            "partner_player_id",
            "won_flag",
            "lost_flag",
            "games_won",
            "games_lost",
            "point_share",
            "point_differential",
            "membership_history_warning_flag",
        ),
    ),
    "resolved_match_teams": (
        "gold",
        "resolved_match_teams",
        (
            "match_id",
            "match_team_id",
            "resolved_team_id",
            "player_one_id",
            "player_two_id",
        ),
    ),
    "teams": (
        "silver",
        "teams",
        (
            "team_id",
            "team_category",
            "country_code",
            "team_status",
            "active_flag",
        ),
    ),
    "team_memberships": (
        "silver",
        "team_memberships",
        (
            "team_id",
            "player_id",
            "current_membership_flag",
            "membership_overlap_flag",
        ),
    ),
    "team_performance_features": (
        "gold",
        "team_performance_features",
        (
            "team_id",
            "evidence_window",
            "candidate_attribution_allowed_flag",
            "match_count",
        ),
    ),
    "competition_match_sides": (
        "gold",
        "competition_match_sides",
        (
            "match_id",
            "match_team_id",
            "team_id",
            "games_won",
            "games_lost",
            "point_share",
            "point_differential",
            "pre_match_team_rating",
            "opponent_pre_match_team_rating",
            "membership_history_warning_flag",
        ),
    ),
}


class Phase8SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 8 fields."""


@dataclass(frozen=True)
class Phase8PublicationSummary:
    """Published-table summary for the Phase 8 target table."""

    entity_data_quality_confidence: EntityDataQualityConfidencePublicationSummary


def validate_phase8_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed source tables expose the required Phase 8 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for logical_name, (layer, table_name, required_columns) in PHASE8_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = (
            get_gold_target_table_fqn(environment, table_name)
            if layer == "gold"
            else get_silver_source_table_fqn(environment, table_name)
        )
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase8SourceContractError(
                f"Phase 8 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[logical_name] = required_columns
    return validated_columns


def publish_phase8_quality_table(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    quality_rules_config: dict[str, Any],
) -> Phase8PublicationSummary:
    """Publish the Gold Phase 8 quality-confidence table."""
    quality_summary = publish_entity_data_quality_confidence(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        quality_rules_config=quality_rules_config,
    )
    return Phase8PublicationSummary(
        entity_data_quality_confidence=quality_summary,
    )
