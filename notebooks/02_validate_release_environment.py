"""Databricks task notebook for validating Raw-to-Bronze release environment."""

# Databricks notebook source

# COMMAND ----------
# Title: 02 Validate Release Environment
# Purpose:
# Resolve Raw-to-Bronze configuration for one release and validate or create the
# required catalog, schemas, and raw volume in Databricks.

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
from napa_pipeline.raw_to_bronze.environment import ensure_release_environment

# COMMAND ----------
dbutils.widgets.dropdown("release_name", "napa_5k", ["napa_5k", "napa_50k", "napa_250k"])
dbutils.widgets.text("config_root", "")
dbutils.widgets.dropdown("create_missing", "true", ["true", "false"])

release_name = dbutils.widgets.get("release_name").strip()
config_root = dbutils.widgets.get("config_root").strip() or None
create_missing = dbutils.widgets.get("create_missing").strip().lower() == "true"

config = load_raw_to_bronze_config(release_name, config_root=config_root)
status = ensure_release_environment(spark, config, create_missing=create_missing)

# COMMAND ----------
print(f"Release name: {config.release_name}")
print(f"Catalog: {status.release_environment.catalog}")
print(f"Raw schema: {status.release_environment.raw_schema}")
print(f"Bronze schema: {status.release_environment.bronze_schema}")
print(f"Operations schema: {status.release_environment.operations_schema}")
print(f"Raw volume: {status.release_environment.raw_volume_fqn}")
print(f"Raw volume path: {status.release_environment.raw_volume_path}")

for schema_status in status.schema_statuses:
    state = "already existed" if schema_status.existed else "created"
    print(f"Schema {state}: {schema_status.object_name}")

volume_state = "already existed" if status.volume_status.existed else "created"
print(f"Volume {volume_state}: {status.volume_status.object_name}")

display(
    spark.createDataFrame(
        [
            {
                "object_type": schema_status.object_type,
                "object_name": schema_status.object_name,
                "existed": schema_status.existed,
            }
            for schema_status in status.schema_statuses
        ]
        + [
            {
                "object_type": status.volume_status.object_type,
                "object_name": status.volume_status.object_name,
                "existed": status.volume_status.existed,
            }
        ]
    )
)
