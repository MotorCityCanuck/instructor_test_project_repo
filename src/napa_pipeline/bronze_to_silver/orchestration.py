"""Orchestration helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from collections import defaultdict

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig


def get_silver_tables_by_stage(
    config: BronzeToSilverConfig,
) -> dict[str, list[dict[str, object]]]:
    """Group enabled Silver tables by configured build stage."""
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for table_config in config.silver_tables_in_build_order:
        grouped[str(table_config["stage"])].append(table_config)
    return dict(grouped)
