"""Validation helpers for the Silver-to-Gold Phase 9 match modeling harness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import get_gold_target_table_fqn
from napa_pipeline.silver_to_gold.match_models import (
    MatchModelMetricsPublicationSummary,
    MatchOutcomePredictionsPublicationSummary,
    MatchOutcomeTrainingSetPublicationSummary,
    Phase9PublicationSummary,
    publish_phase9_modeling_tables,
)


PHASE9_REQUIRED_SOURCE_COLUMNS: dict[str, tuple[str, tuple[str, ...]]] = {
    "competition_match_sides": (
        "competition_match_sides",
        (
            "match_id",
            "match_team_id",
            "match_date",
            "batch_id",
            "batch_sequence",
            "batch_date",
            "region_id",
            "match_country_code",
            "match_type",
            "competition_category",
            "team_number",
            "team_id",
            "winning_team_number",
            "completed_flag",
            "pre_match_team_rating",
        ),
    ),
    "team_performance_features": (
        "team_performance_features",
        (
            "team_id",
            "evidence_window",
            "recent_form_win_pct",
            "shrinkage_adjusted_win_rate",
            "strength_of_schedule",
            "partnership_duration_days",
            "consistency_score",
            "evidence_reliability_score",
            "feature_evidence_status",
        ),
    ),
}


class Phase9SourceContractError(RuntimeError):
    """Raised when the deployed source contract is missing required Phase 9 fields."""


@dataclass(frozen=True)
class Phase9PublishedTables:
    """Published-table summary for the Phase 9 target tables."""

    training_set: MatchOutcomeTrainingSetPublicationSummary
    predictions: MatchOutcomePredictionsPublicationSummary
    metrics: MatchModelMetricsPublicationSummary


def validate_phase9_source_contract(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, tuple[str, ...]]:
    """Validate that the deployed Gold source tables expose required Phase 9 columns."""
    validated_columns: dict[str, tuple[str, ...]] = {}
    for logical_name, (table_name, required_columns) in PHASE9_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = get_gold_target_table_fqn(environment, table_name)
        schema = spark.table(table_fqn).schema
        actual_columns = {field.name for field in getattr(schema, "fields", [])}
        missing_columns = [column for column in required_columns if column not in actual_columns]
        if missing_columns:
            raise Phase9SourceContractError(
                f"Phase 9 source contract validation failed for {table_fqn}: "
                f"missing columns {', '.join(missing_columns)}."
            )
        validated_columns[logical_name] = required_columns
    return validated_columns


def publish_phase9_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    analysis_as_of_date: date,
    models_config: dict[str, Any],
    model_run_id: str,
    model_name: str,
    model_version: str,
    feature_definition_version: str,
) -> Phase9PublicationSummary:
    """Publish the Gold Phase 9 training, prediction, and metric tables."""
    return publish_phase9_modeling_tables(
        spark,
        environment,
        analysis_as_of_date=analysis_as_of_date,
        models_config=models_config,
        model_run_id=model_run_id,
        model_name=model_name,
        model_version=model_version,
        feature_definition_version=feature_definition_version,
    )
