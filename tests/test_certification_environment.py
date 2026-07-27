"""Tests for Raw certification environment resolution."""

from napa_pipeline.certification.config import load_certification_config
from napa_pipeline.certification.environment import resolve_release_environment


def test_resolve_release_environment_for_50k_release() -> None:
    config = load_certification_config("50k")

    environment = resolve_release_environment(config)

    assert environment.catalog == "workspace"
    assert environment.raw_schema == "instructor_50k_raw"
    assert environment.operations_schema == "instructor_ops"
    assert environment.raw_volume_name == "napa_files"
    assert environment.raw_volume_fqn == "workspace.instructor_50k_raw.napa_files"
    assert environment.artifacts_volume_name == "certification_artifacts"
    assert (
        environment.artifacts_volume_fqn
        == "workspace.instructor_ops.certification_artifacts"
    )
    assert environment.artifacts_root_path.endswith("/raw_certification/napa_50k")
