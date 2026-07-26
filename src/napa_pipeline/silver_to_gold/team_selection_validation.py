"""Validation helpers for the Silver-to-Gold Phase 11 team selection harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.team_selection import (
    OlympicTeamCandidatesPublicationSummary,
    Phase11PublicationSummary,
    TeamSelectionScorecardsPublicationSummary,
    publish_phase11_team_tables,
)


PHASE11_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "teams": (
        "silver",
        "teams",
        (
            "team_id",
            "team_category",
            "country_code",
            "team_status",
            "active_flag",
            "formation_date",
            "dissolution_date",
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
            "membership_start_date",
            "membership_end_date",
        ),
    ),
    "team_performance_features": (
        "gold",
        "team_performance_features",
        (
            "team_id",
            "evidence_window",
            "team_category",
            "country_code",
            "candidate_attribution_allowed_flag",
            "shrinkage_adjusted_win_rate",
            "recent_form_win_pct",
            "performance_above_expectation",
            "consistency_score",
            "partnership_duration_days",
            "evidence_reliability_score",
            "feature_evidence_status",
        ),
    ),
    "partnership_effectiveness": (
        "gold",
        "partnership_effectiveness",
        (
            "partnership_key",
            "team_id",
            "player_one_id",
            "player_two_id",
            "team_adjusted_win_rate",
            "synergy_proxy",
            "partnership_duration_days",
            "evidence_reliability_score",
            "feature_evidence_status",
            "candidate_attribution_allowed_flag",
        ),
    ),
    "player_evaluation_scorecards": (
        "gold",
        "player_evaluation_scorecards",
        (
            "player_id",
            "scoring_scenario",
            "display_name",
            "country_code",
            "confidence_adjusted_player_score",
            "combined_confidence_score",
            "evidence_band",
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
    "resolved_match_teams": (
        "gold",
        "resolved_match_teams",
        (
            "match_id",
            "match_date",
            "resolved_team_id",
            "team_resolution_confidence",
        ),
    ),
    "match_outcome_predictions": (
        "gold",
        "match_outcome_predictions",
        (
            "match_id",
            "match_date",
            "team_a_team_id",
            "team_b_team_id",
            "model_predicted_probability",
        ),
    ),
}


class Phase11SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 11 fields."""


@dataclass(frozen=True)
class Phase11PublishedTables:
    """Published-table summary for the Phase 11 target tables."""

    team_selection_scorecards: TeamSelectionScorecardsPublicationSummary
    olympic_team_candidates: OlympicTeamCandidatesPublicationSummary


def validate_phase11_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed source tables expose required Phase 11 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for logical_name, (layer, table_name, required_columns) in PHASE11_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = (
            get_gold_target_table_fqn(environment, table_name)
            if layer == "gold"
            else get_silver_source_table_fqn(environment, table_name)
        )
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase11SourceContractError(
                f"Phase 11 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[logical_name] = required_columns
    return validated_columns


def publish_phase11_selection_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    scoring_scenario: str,
    scorecards_config: dict[str, Any],
    eligibility_config: dict[str, Any],
) -> Phase11PublicationSummary:
    """Publish the Gold Phase 11 team selection tables."""
    return publish_phase11_team_tables(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        scoring_scenario=scoring_scenario,
        scorecards_config=scorecards_config,
        eligibility_config=eligibility_config,
    )
