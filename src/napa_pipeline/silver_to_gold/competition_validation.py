"""Validation helpers for the Silver-to-Gold Phase 3 competition harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.competition import (
    CompetitionMatchSidesPublicationSummary,
    CompetitionPlayerMatchesPublicationSummary,
    publish_competition_match_sides,
    publish_competition_player_matches,
)
from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import get_silver_source_table_fqn


PHASE3_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "matches": (
        "match_id",
        "batch_id",
        "region_id",
        "match_date",
        "match_type",
        "competition_category",
        "winning_team_id",
        "winning_team_number",
        "completed_flag",
    ),
    "match_teams": (
        "match_team_id",
        "match_id",
        "team_id",
        "team_number",
        "pre_match_team_rating",
        "side_cardinality_warning_flag",
    ),
    "match_team_players": (
        "match_team_id",
        "match_id",
        "player_id",
        "player_position",
        "player_rating_at_match",
        "membership_history_warning_flag",
    ),
    "match_games": (
        "match_id",
        "team_one_score",
        "team_two_score",
        "winning_team_number",
        "close_game_flag",
    ),
    "regions": (
        "region_id",
        "country_code",
    ),
    "monthly_batches": (
        "batch_id",
        "batch_sequence",
        "batch_date",
    ),
}


class Phase3SourceContractError(RuntimeError):
    """Raised when the deployed Silver source contract is missing required Phase 3 fields."""


@dataclass(frozen=True)
class Phase3PublicationSummary:
    """Published-table summaries for the two Phase 3 target tables."""

    competition_match_sides: CompetitionMatchSidesPublicationSummary
    competition_player_matches: CompetitionPlayerMatchesPublicationSummary


def validate_phase3_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed Silver source tables expose the required Phase 3 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for table_name, required_columns in PHASE3_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = get_silver_source_table_fqn(environment, table_name)
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase3SourceContractError(
                f"Phase 3 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[table_name] = required_columns
    return validated_columns


def publish_phase3_competition_foundation(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
) -> Phase3PublicationSummary:
    """Publish the two Gold Phase 3 competition foundation tables."""
    match_sides_summary = publish_competition_match_sides(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
    )
    player_matches_summary = publish_competition_player_matches(spark, environment)
    return Phase3PublicationSummary(
        competition_match_sides=match_sides_summary,
        competition_player_matches=player_matches_summary,
    )
