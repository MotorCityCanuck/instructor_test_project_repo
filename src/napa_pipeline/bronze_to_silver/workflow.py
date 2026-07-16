"""Workflow-support helpers for Bronze-to-Silver Databricks script tasks."""

from __future__ import annotations

from typing import Any

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.environment import ReleaseEnvironment
from napa_pipeline.bronze_to_silver.io import get_bronze_source_table_fqn
from napa_pipeline.bronze_to_silver.orchestration import get_silver_tables_by_stage


CONVENIENCE_VIEW_NAMES = (
    "vw_players_current",
    "vw_team_rosters",
    "vw_current_team_memberships",
    "vw_match_results",
    "vw_player_match_history",
)


def validate_bronze_source_tables(
    spark: Any,
    config: BronzeToSilverConfig,
    environment: ReleaseEnvironment,
) -> tuple[list[str], list[str]]:
    """Return existing and missing configured Bronze source tables."""
    existing_tables: list[str] = []
    missing_tables: list[str] = []
    for source_name, source_config in sorted(config.enabled_sources.items()):
        table_fqn = get_bronze_source_table_fqn(environment, source_config)
        if spark.catalog.tableExists(table_fqn):
            existing_tables.append(table_fqn)
        else:
            missing_tables.append(f"{source_name}:{table_fqn}")
    return existing_tables, missing_tables


def get_stage_table_names(
    config: BronzeToSilverConfig,
    stage_name: str,
) -> list[str]:
    """Return enabled Silver target names for one configured build stage."""
    grouped = get_silver_tables_by_stage(config)
    return [str(table["target"]) for table in grouped.get(stage_name, [])]
