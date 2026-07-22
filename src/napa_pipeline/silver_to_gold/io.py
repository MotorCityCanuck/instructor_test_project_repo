"""I/O naming helpers for the Silver-to-Gold pipeline."""

from __future__ import annotations

from napa_pipeline.silver_to_gold.environment import GoldRuntimeContext, ReleaseEnvironment


def get_silver_source_table_fqn(
    environment: ReleaseEnvironment,
    table_name: str,
) -> str:
    """Return the fully qualified Silver source table name."""
    return f"{environment.catalog}.{environment.silver_schema}.{table_name}"


def get_gold_target_table_fqn(
    environment: ReleaseEnvironment,
    table_name: str,
) -> str:
    """Return the fully qualified Gold target table name."""
    return f"{environment.catalog}.{environment.gold_schema}.{table_name}"


def get_gold_stage_table_fqn(
    environment: ReleaseEnvironment,
    table_name: str,
) -> str:
    """Return the fully qualified Gold stage table name."""
    return f"{environment.catalog}.{environment.gold_stage_schema}.{table_name}"


def get_operations_table_fqn(
    context: GoldRuntimeContext,
    table_name: str,
) -> str:
    """Return the fully qualified operations table name."""
    return f"{context.catalog}.{context.operations_schema}.{table_name}"

