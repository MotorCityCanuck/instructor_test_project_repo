"""Validation helpers for the Silver-to-Gold Phase 7 team feature harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.team_features import (
    PartnershipEffectivenessPublicationSummary,
    TeamPerformanceFeaturesPublicationSummary,
    publish_partnership_effectiveness,
    publish_team_performance_features,
)


PHASE7_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "resolved_match_teams": (
        "gold",
        "resolved_match_teams",
        (
            "match_id",
            "match_team_id",
            "match_date",
            "player_one_id",
            "player_two_id",
            "canonical_player_pair_key",
            "resolved_team_id",
            "candidate_attribution_allowed_flag",
        ),
    ),
    "competition_match_sides": (
        "gold",
        "competition_match_sides",
        (
            "match_id",
            "match_team_id",
            "match_date",
            "won_flag",
            "lost_flag",
            "games_won",
            "games_lost",
            "point_share",
            "point_differential",
            "pre_match_team_rating",
            "opponent_pre_match_team_rating",
            "close_game_count",
            "deciding_game_flag",
            "membership_history_warning_flag",
        ),
    ),
    "player_current_ratings": (
        "gold",
        "player_current_ratings",
        (
            "player_id",
            "analytical_rating_value",
        ),
    ),
    "player_performance_features": (
        "gold",
        "player_performance_features",
        (
            "player_id",
            "evidence_window",
            "win_pct",
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
            "formation_date",
            "dissolution_date",
            "active_flag",
        ),
    ),
    "team_memberships": (
        "silver",
        "team_memberships",
        (
            "team_id",
            "player_id",
            "membership_overlap_flag",
        ),
    ),
}


class Phase7SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 7 fields."""


@dataclass(frozen=True)
class Phase7PublicationSummary:
    """Published-table summaries for the two Phase 7 target tables."""

    team_performance_features: TeamPerformanceFeaturesPublicationSummary
    partnership_effectiveness: PartnershipEffectivenessPublicationSummary


def validate_phase7_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed source tables expose the required Phase 7 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for logical_name, (layer, table_name, required_columns) in PHASE7_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = (
            get_gold_target_table_fqn(environment, table_name)
            if layer == "gold"
            else get_silver_source_table_fqn(environment, table_name)
        )
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase7SourceContractError(
                f"Phase 7 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[logical_name] = required_columns
    return validated_columns


def publish_phase7_team_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    features_config: dict[str, Any],
    evidence_windows_config: dict[str, Any],
) -> Phase7PublicationSummary:
    """Publish the two Gold Phase 7 team feature tables."""
    team_summary = publish_team_performance_features(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        features_config=features_config,
        evidence_windows_config=evidence_windows_config,
    )
    partnership_summary = publish_partnership_effectiveness(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        evidence_windows_config=evidence_windows_config,
    )
    return Phase7PublicationSummary(
        team_performance_features=team_summary,
        partnership_effectiveness=partnership_summary,
    )
