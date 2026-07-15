"""Databricks task notebook for resolving Raw-to-Bronze configuration."""

# Databricks notebook source

# COMMAND ----------
# Title: 01 Resolve Configuration
# Purpose:
# Resolve and validate the Raw-to-Bronze YAML configuration for one release and
# display the key settings needed by downstream Databricks workflow tasks.

from pathlib import Path


def _load_bootstrap_helper() -> None:
    """Load the shared notebook bootstrap helper."""
    search_roots = []

    if "__file__" in globals():
        search_roots.append(Path(__file__).resolve().parent)

    current_dir = Path.cwd().resolve()
    search_roots.extend([current_dir, *current_dir.parents])

    for root in search_roots:
        for candidate in (
            root / "_bootstrap_napa_pipeline.py",
            root / "notebooks" / "_bootstrap_napa_pipeline.py",
        ):
            if candidate.exists():
                exec(candidate.read_text(), globals())
                return

    raise FileNotFoundError(
        "Could not locate '_bootstrap_napa_pipeline.py'. "
        "Run this notebook from the repository workspace."
    )


_load_bootstrap_helper()
bootstrap_napa_pipeline_imports()

from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.operations import create_pipeline_context

# COMMAND ----------
dbutils.widgets.dropdown("release_name", "napa_5k", ["napa_5k", "napa_50k", "napa_250k"])
dbutils.widgets.text("config_root", "")
dbutils.widgets.text("pipeline_run_id", "")

release_name = dbutils.widgets.get("release_name").strip()
config_root = dbutils.widgets.get("config_root").strip() or None
pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip() or None

config = load_raw_to_bronze_config(release_name, config_root=config_root)
environment = resolve_release_environment(config)
context = create_pipeline_context(
    config,
    environment,
    pipeline_run_id=pipeline_run_id,
)

# COMMAND ----------
enabled_sources = config.sources_in_build_order

print(f"Pipeline name: {config.data['project']['pipeline_name']}")
print(f"Pipeline version: {config.data['project']['pipeline_version']}")
print(f"Release name: {config.release_name}")
print(f"Config root: {config.config_root}")
print(f"Config hash: {config.config_hash}")
print(f"Pipeline run ID: {context.pipeline_run_id}")
print(f"Catalog: {config.data['runtime']['catalog']}")
print(f"Raw schema: {config.data['schemas']['raw']}")
print(f"Bronze schema: {config.data['schemas']['bronze']}")
print(f"Operations schema: {config.data['schemas']['operations']}")
print(f"Raw volume path: {config.data['volume']['path']}")
print(f"Enabled source count: {len(enabled_sources)}")

try:
    dbutils.jobs.taskValues.set(key="pipeline_run_id", value=context.pipeline_run_id)
    dbutils.jobs.taskValues.set(key="config_hash", value=config.config_hash)
except Exception:
    pass

display(
    spark.createDataFrame(
        [
            {
                "source_name": source["source_name"],
                "file_name": source["file_name"],
                "bronze_table": source["bronze_table"],
                "build_order": source["build_order"],
            }
            for source in enabled_sources
        ]
    )
)
