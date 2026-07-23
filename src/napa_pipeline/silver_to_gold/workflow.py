"""Workflow-support helpers for Silver-to-Gold Databricks script tasks."""

from __future__ import annotations

from typing import Any

from napa_pipeline.silver_to_gold.config import SilverToGoldConfig
from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import get_silver_source_table_fqn
from napa_pipeline.silver_to_gold.operations import (
    PIPELINE_RUNS_TABLE,
    PipelineContext,
    append_records,
    build_pipeline_run_start_record,
    ensure_operations_tables,
)


REQUIRED_SILVER_SOURCE_TABLES = (
    "monthly_batches",
    "regions",
    "clubs",
    "club_memberships",
    "players",
    "player_registrations",
    "player_assessment_history",
    "teams",
    "team_memberships",
    "matches",
    "match_teams",
    "match_team_players",
    "match_games",
)

PHASE3_TARGET_TABLES = (
    "competition_match_sides",
    "competition_player_matches",
)

PHASE4_TARGET_TABLES = (
    "resolved_match_teams",
)


class UpstreamSilverRunNotFoundError(RuntimeError):
    """Raised when no successful upstream Bronze-to-Silver run can be resolved."""


class SilverSourceValidationError(RuntimeError):
    """Raised when required Silver source tables are missing."""


def resolve_latest_successful_upstream_run_id(
    spark: Any,
    config: SilverToGoldConfig,
    environment: ReleaseEnvironment,
) -> str:
    """Return the latest successful Bronze-to-Silver run ID for the selected release."""
    pipeline_runs_table = str(config.data["source_contract"]["upstream_pipeline_runs_table"])
    expected_pipeline_name = str(config.data["source_contract"]["expected_upstream_pipeline_name"])
    table_fqn = f"{environment.catalog}.{environment.operations_schema}.{pipeline_runs_table}"
    query = f"""
SELECT pipeline_run_id
FROM {table_fqn}
WHERE pipeline_name = '{expected_pipeline_name}'
  AND release_name = '{config.release_name}'
  AND status = 'SUCCEEDED'
ORDER BY completed_ts DESC, started_ts DESC
LIMIT 1
""".strip()
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:
        raise UpstreamSilverRunNotFoundError(
            f"Could not query upstream Silver runs from {table_fqn}."
        ) from exc

    if not rows:
        raise UpstreamSilverRunNotFoundError(
            "No successful Bronze-to-Silver run was found for "
            f"release_name '{config.release_name}' in {table_fqn}."
        )

    row = rows[0]
    mapping = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    pipeline_run_id = str(mapping.get("pipeline_run_id") or "").strip()
    if not pipeline_run_id:
        raise UpstreamSilverRunNotFoundError(
            f"Latest upstream Silver run record in {table_fqn} did not contain pipeline_run_id."
        )
    return pipeline_run_id


def validate_required_silver_source_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    required_table_names: tuple[str, ...] = REQUIRED_SILVER_SOURCE_TABLES,
) -> tuple[list[str], list[str]]:
    """Return existing and missing required Silver source tables."""
    existing_tables: list[str] = []
    missing_tables: list[str] = []
    for table_name in required_table_names:
        table_fqn = get_silver_source_table_fqn(environment, table_name)
        if spark.catalog.tableExists(table_fqn):
            existing_tables.append(table_fqn)
        else:
            missing_tables.append(table_fqn)
    return existing_tables, missing_tables


def require_required_silver_source_tables(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    required_table_names: tuple[str, ...] = REQUIRED_SILVER_SOURCE_TABLES,
) -> list[str]:
    """Validate that every required Silver source table exists and return the existing FQNs."""
    existing_tables, missing_tables = validate_required_silver_source_tables(
        spark,
        environment,
        required_table_names=required_table_names,
    )
    if missing_tables:
        raise SilverSourceValidationError(
            "Missing required Silver source tables: " + ", ".join(missing_tables)
        )
    return existing_tables


def collect_match_rows_for_analysis_date(
    spark: Any,
    environment: ReleaseEnvironment,
) -> list[dict[str, Any]]:
    """Collect the minimal match fields needed to resolve analysis_as_of_date."""
    matches_fqn = get_silver_source_table_fqn(environment, "matches")
    rows = spark.sql(
        f"""
SELECT
    match_date
FROM {matches_fqn}
""".strip()
    ).collect()
    return [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in rows
    ]


def initialize_pipeline_run(
    spark: Any,
    context: PipelineContext,
) -> None:
    """Ensure operations tables exist and append the Gold pipeline start record once."""
    ensure_operations_tables(spark, context)
    existing_rows = [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in spark.table(f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}").toLocalIterator()
        if (row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)).get("pipeline_run_id")
        == context.pipeline_run_id
    ]
    if existing_rows:
        return
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}",
        [build_pipeline_run_start_record(context)],
    )
