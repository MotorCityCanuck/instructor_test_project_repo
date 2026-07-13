"""Shared pipeline configuration helpers for Databricks notebooks and scripts."""

from dataclasses import dataclass
from typing import Any, Mapping


WIDGET_DEFAULTS = {
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


@dataclass(frozen=True)
class PipelineConfig:
    """Normalized pipeline configuration used across notebook stages."""

    catalog: str
    raw_schema: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    ops_schema: str
    raw_volume: str
    dataset_name: str
    source_path_override: str = ""

    @property
    def source_path(self) -> str:
        """Return the active raw dataset path."""
        if self.source_path_override:
            return self.source_path_override
        return (
            f"/Volumes/{self.catalog}/{self.raw_schema}/"
            f"{self.raw_volume}/{self.dataset_name}"
        )


def _get_required_value(values: Mapping[str, Any], key: str) -> str:
    value = str(values.get(key, "")).strip()
    if not value and key != "source_path":
        raise ValueError(f"Required configuration value '{key}' is empty.")
    return value


def build_pipeline_config(values: Mapping[str, Any]) -> PipelineConfig:
    """Build a validated pipeline config from a mapping of raw values."""
    config = PipelineConfig(
        catalog=_get_required_value(values, "catalog"),
        raw_schema=_get_required_value(values, "raw_schema"),
        bronze_schema=_get_required_value(values, "bronze_schema"),
        silver_schema=_get_required_value(values, "silver_schema"),
        gold_schema=_get_required_value(values, "gold_schema"),
        ops_schema=_get_required_value(values, "ops_schema"),
        raw_volume=_get_required_value(values, "raw_volume"),
        dataset_name=str(values.get("dataset_name", "")).strip(),
        source_path_override=str(values.get("source_path", "")).strip(),
    )

    if not config.dataset_name and not config.source_path_override:
        raise ValueError(
            "Provide either 'dataset_name' or an explicit 'source_path' value."
        )

    return config


def register_pipeline_widgets(dbutils: Any) -> None:
    """Create the standard widget set used across the pipeline notebooks."""
    for name, default_value in WIDGET_DEFAULTS.items():
        dbutils.widgets.text(name, default_value)


def load_pipeline_config(dbutils: Any) -> PipelineConfig:
    """Load pipeline configuration from the standard Databricks widget set."""
    raw_values = {
        name: dbutils.widgets.get(name)
        for name in WIDGET_DEFAULTS
    }
    return build_pipeline_config(raw_values)
