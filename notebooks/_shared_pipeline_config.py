"""Databricks notebook helper for shared pipeline widget configuration."""

# Databricks notebook source

# COMMAND ----------
HELPER_VERSION = "2026.07.15.1"

PIPELINE_WIDGET_DEFAULTS = {
    "catalog": "workspace",
    "raw_schema": "instructor_raw",
    "bronze_schema": "instructor_bronze",
    "silver_schema": "instructor_silver",
    "gold_schema": "instructor_gold",
    "ops_schema": "instructor_ops",
    "raw_volume": "napa_files",
    "dataset_name": "napa_5k",
    "source_path": "",
}


def register_pipeline_widgets(dbutils):
    """Create the standard widget set used across the pipeline notebooks."""
    for name, default_value in PIPELINE_WIDGET_DEFAULTS.items():
        dbutils.widgets.text(name, default_value)


def load_pipeline_config(dbutils):
    """Load and validate the standard pipeline widget values."""
    config = {
        name: dbutils.widgets.get(name).strip()
        for name in PIPELINE_WIDGET_DEFAULTS
    }

    for key in [
        "catalog",
        "raw_schema",
        "bronze_schema",
        "silver_schema",
        "gold_schema",
        "ops_schema",
        "raw_volume",
    ]:
        if not config[key]:
            raise ValueError(f"Required configuration value '{key}' is empty.")

    if not config["dataset_name"] and not config["source_path"]:
        raise ValueError(
            "Provide either 'dataset_name' or an explicit 'source_path' value."
        )

    if config["source_path"]:
        config["resolved_source_path"] = config["source_path"]
    else:
        config["resolved_source_path"] = (
            f"/Volumes/{config['catalog']}/{config['raw_schema']}/"
            f"{config['raw_volume']}/{config['dataset_name']}"
        )

    return config
