"""Tests for Raw certification configuration loading."""

from pathlib import Path

import pytest
import yaml

from napa_pipeline.certification.config import (
    CertificationConfigError,
    get_default_config_root,
    load_certification_config,
)


def test_get_default_config_root_points_to_certification_config() -> None:
    root = get_default_config_root()

    assert root.name == "certification"
    assert root.parent.name == "config"


def test_load_certification_config_accepts_short_alias() -> None:
    config = load_certification_config("5k")

    assert config.release_name == "napa_5k"
    assert config.release_role == "development"
    assert config.data["project"]["pipeline_name"] == "raw_certification"
    assert config.data["volumes"]["raw"]["path"].endswith("/instructor_5k_raw/napa_files")
    assert config.data["artifacts"]["root_path"].endswith(
        "/instructor_ops/certification_artifacts/raw_certification/napa_5k"
    )
    assert len(config.sources_in_build_order) == 13
    assert config.sources_in_build_order[0]["source_name"] == "regions"
    assert config.threshold_statuses["minimum_active_player_rate_blocker"] == "provisional"


def test_load_certification_config_accepts_canonical_release_name() -> None:
    config = load_certification_config("napa_250k")

    assert config.release_name == "napa_250k"
    assert config.release_role == "production"


def test_load_certification_config_rejects_release_mismatch(tmp_path: Path) -> None:
    config_root = tmp_path / "certification"
    (config_root / "environments").mkdir(parents=True)
    sources_path = tmp_path / "raw_sources.yml"

    (config_root / "base.yml").write_text(
        yaml.safe_dump(
            {
                "project": {
                    "name": "test",
                    "pipeline_name": "raw_certification",
                    "pipeline_version": "1.0.0",
                    "processing_mode": "certification",
                },
                "runtime": {"catalog": "workspace", "team_prefix": "instructor"},
                "objects": {
                    "raw_volume_name": "napa_files",
                    "operations_schema": "instructor_ops",
                    "artifacts_volume_name": "certification_artifacts",
                },
                "execution": {"fail_fast": False},
                "manifest": {"enabled": True, "optional": True, "file_names": ["manifest.json"]},
                "profiles": {
                    "common": {
                        "minimum_active_player_rate_blocker": 0.60,
                        "maximum_zero_match_active_player_rate": 0.35,
                        "minimum_player_rating_coverage_rate": 0.70,
                        "minimum_confidence_coverage_rate": 0.50,
                        "minimum_development_players_with_history": 1,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (config_root / "environments" / "napa_5k.yml").write_text(
        yaml.safe_dump(
            {
                "release": {"release_name": "napa_50k", "role": "development"},
                "schemas": {"raw": "instructor_5k_raw", "operations": "instructor_ops"},
                "volumes": {
                    "raw": {"name": "napa_files", "path": "/Volumes/workspace/instructor_5k_raw/napa_files"},
                    "artifacts": {
                        "name": "certification_artifacts",
                        "path": "/Volumes/workspace/instructor_ops/certification_artifacts",
                    },
                },
                "artifacts": {
                    "root_path": "/Volumes/workspace/instructor_ops/certification_artifacts/raw_certification/napa_5k"
                },
                "performance": {"shuffle_partitions": 16},
                "profiles": {
                    "release_specific": {
                        "minimum_viable_teams_per_country_division": 10,
                        "minimum_recent_matches_per_candidate_team": 3,
                        "minimum_assessment_periods_for_development_probe": 2,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    sources_path.write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "players": {
                        "enabled": True,
                        "file_name": "players.parquet",
                        "build_order": 10,
                        "key_columns": ["id"],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CertificationConfigError, match="does not match"):
        load_certification_config("5k", config_root=config_root, sources_config_path=sources_path)


def test_load_certification_config_includes_calibration_metadata() -> None:
    config = load_certification_config("napa_250k")

    calibration = config.data["calibration"]
    assert "required_cases" in calibration
    assert "approved_baselines" in calibration
    assert calibration["approved_baselines"]["napa_250k"]["status"] == "provisional"
