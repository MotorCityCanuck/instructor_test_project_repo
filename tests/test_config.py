"""Tests for shared pipeline configuration helpers."""

import pytest

from napa_pipeline.config import WIDGET_DEFAULTS, build_pipeline_config


def test_build_pipeline_config_uses_default_volume_path() -> None:
    config = build_pipeline_config(WIDGET_DEFAULTS)

    assert config.catalog == "workspace"
    assert config.raw_schema == "instructor_raw"
    assert config.bronze_schema == "instructor_bronze"
    assert config.source_path == "/Volumes/workspace/instructor_raw/napa_files/napa_5k"


def test_build_pipeline_config_prefers_explicit_source_path() -> None:
    config = build_pipeline_config(
        {
            **WIDGET_DEFAULTS,
            "source_path": "/Volumes/workspace/instructor_raw/napa_files/custom_run",
        }
    )

    assert config.source_path == (
        "/Volumes/workspace/instructor_raw/napa_files/custom_run"
    )


def test_build_pipeline_config_requires_dataset_or_source_path() -> None:
    with pytest.raises(ValueError, match="dataset_name"):
        build_pipeline_config({**WIDGET_DEFAULTS, "dataset_name": ""})
