"""Validation and publication helpers for Gold contextual adjustment features."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.context_adjustments import (
    TeamContextAdjustmentFeaturesPublicationSummary,
    publish_team_context_adjustment_features,
)
from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)


PHASE_CONTEXT_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "players": ("silver", "players", ("player_id", "country_code", "home_region_id", "age")),
    "teams": ("silver", "teams", ("team_id",)),
    "team_memberships": (
        "silver",
        "team_memberships",
        ("team_id", "player_id", "current_membership_flag", "membership_start_date", "membership_end_date"),
    ),
    "competition_match_sides": (
        "gold",
        "competition_match_sides",
        ("match_id", "match_date", "completed_flag", "won_flag", "pre_match_team_rating", "opponent_pre_match_team_rating", "player_one_id", "player_two_id", "side_cardinality_warning_flag"),
    ),
    "competition_player_matches": (
        "gold",
        "competition_player_matches",
        ("match_id", "match_date", "team_id", "player_id", "points_for", "points_against"),
    ),
}


class ContextAdjustmentSourceContractError(RuntimeError):
    """Raised when contextual Gold features lack governed source fields."""


@dataclass(frozen=True)
class ContextAdjustmentPublicationSummary:
    """Published-table summary for the context-adjustment stage."""

    team_context_adjustment_features: TeamContextAdjustmentFeaturesPublicationSummary


def validate_context_adjustment_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate the Silver and Gold contracts used for contextual factors."""
    validated: dict[str, tuple[str, ...]] = {}
    for logical_name, (layer, table_name, required_columns) in PHASE_CONTEXT_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = (
            get_gold_target_table_fqn(environment, table_name)
            if layer == "gold"
            else get_silver_source_table_fqn(environment, table_name)
        )
        actual_columns = {field.name for field in spark.table(table_fqn).schema.fields}
        missing = [column for column in required_columns if column not in actual_columns]
        if missing:
            raise ContextAdjustmentSourceContractError(
                f"Context-adjustment source contract failed for {table_fqn}: missing columns {', '.join(missing)}."
            )
        validated[logical_name] = required_columns
    return validated


def publish_context_adjustment_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    context_config: dict[str, Any],
) -> ContextAdjustmentPublicationSummary:
    """Publish the contextual team feature table."""
    return ContextAdjustmentPublicationSummary(
        team_context_adjustment_features=publish_team_context_adjustment_features(
            spark,
            environment,
            analysis_as_of_date=analysis_as_of_date,
            context_config=context_config,
        )
    )
