"""Validation helpers for the Silver-to-Gold Phase 5 rating-engine harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import (
    get_gold_target_table_fqn,
    get_silver_source_table_fqn,
)
from napa_pipeline.silver_to_gold.ratings import (
    PlayerCurrentRatingsPublicationSummary,
    PlayerRatingEventsPublicationSummary,
    PlayerRatingHistoryPublicationSummary,
    publish_player_current_ratings,
    publish_player_rating_events,
    publish_player_rating_history,
)


PHASE5_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "competition_player_matches": (
        "gold",
        "competition_player_matches",
        (
            "match_id",
            "match_date",
            "batch_id",
            "batch_sequence",
            "batch_date",
            "team_number",
            "player_id",
            "partner_player_id",
            "pre_match_player_rating",
            "won_flag",
            "lost_flag",
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
            "rating",
            "rating_confidence",
        ),
    ),
    "monthly_batches": (
        "silver",
        "monthly_batches",
        (
            "batch_id",
            "batch_sequence",
            "batch_date",
        ),
    ),
}


class Phase5SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 5 fields."""


@dataclass(frozen=True)
class Phase5PublicationSummary:
    """Published-table summaries for the three Phase 5 target tables."""

    player_rating_events: PlayerRatingEventsPublicationSummary
    player_rating_history: PlayerRatingHistoryPublicationSummary
    player_current_ratings: PlayerCurrentRatingsPublicationSummary


def validate_phase5_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed source tables expose the required Phase 5 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for logical_name, (layer, table_name, required_columns) in PHASE5_REQUIRED_SOURCE_COLUMNS.items():
        if layer == "gold":
            table_fqn = get_gold_target_table_fqn(environment, table_name)
        else:
            table_fqn = get_silver_source_table_fqn(environment, table_name)
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase5SourceContractError(
                f"Phase 5 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[logical_name] = required_columns
    return validated_columns


def publish_phase5_rating_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    ratings_config: dict[str, Any],
) -> Phase5PublicationSummary:
    """Publish the three Gold Phase 5 rating tables."""
    events_summary = publish_player_rating_events(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        ratings_config=ratings_config,
    )
    history_summary = publish_player_rating_history(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        ratings_config=ratings_config,
    )
    current_summary = publish_player_current_ratings(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        ratings_config=ratings_config,
    )
    return Phase5PublicationSummary(
        player_rating_events=events_summary,
        player_rating_history=history_summary,
        player_current_ratings=current_summary,
    )
