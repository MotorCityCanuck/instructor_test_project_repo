"""I/O naming helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.environment import ReleaseEnvironment


def get_bronze_source_table_fqn(
    environment: ReleaseEnvironment,
    source_config: dict[str, object],
) -> str:
    """Return the fully qualified Bronze source table name."""
    return (
        f"{environment.catalog}.{environment.bronze_schema}."
        f"{source_config['bronze_table']}"
    )


def get_silver_target_table_fqn(
    environment: ReleaseEnvironment,
    table_config: dict[str, object],
) -> str:
    """Return the fully qualified Silver target table name."""
    return (
        f"{environment.catalog}.{environment.silver_schema}."
        f"{table_config['target']}"
    )


def get_silver_reject_table_fqn(
    environment: ReleaseEnvironment,
    table_config: dict[str, object],
) -> str:
    """Return the fully qualified Silver reject table name."""
    return (
        f"{environment.catalog}.{environment.silver_reject_schema}."
        f"{table_config['reject_table']}"
    )


def get_enabled_sources_by_name(
    config: BronzeToSilverConfig,
) -> dict[str, dict[str, object]]:
    """Return enabled Bronze sources keyed by source name."""
    return dict(config.enabled_sources)
