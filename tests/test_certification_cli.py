"""Tests for Raw certification script task CLI helpers."""

import argparse

import pytest

from napa_pipeline.certification.cli import add_release_argument
from napa_pipeline.certification.config import (
    ALLOWED_RELEASE_ALIASES,
    CertificationConfigError,
    normalize_release_name,
)


def test_normalize_release_name_maps_supported_aliases() -> None:
    assert normalize_release_name("5k") == "napa_5k"
    assert normalize_release_name("50k") == "napa_50k"
    assert normalize_release_name("250k") == "napa_250k"
    assert normalize_release_name("napa_5k") == "napa_5k"
    assert normalize_release_name("NAPA_50K") == "napa_50k"


def test_normalize_release_name_rejects_unsupported_value() -> None:
    with pytest.raises(CertificationConfigError, match="Unsupported release"):
        normalize_release_name("1m")


def test_release_parser_accepts_short_and_full_release_aliases() -> None:
    parser = argparse.ArgumentParser()
    add_release_argument(parser)

    parsed_short = parser.parse_args(["--release-type", "5k"])
    parsed_full = parser.parse_args(["--release-type", "napa_50k"])

    assert parsed_short.release_type == "napa_5k"
    assert parsed_full.release_type == "napa_50k"
    assert "napa_250k" in ALLOWED_RELEASE_ALIASES


def test_release_parser_exits_on_invalid_value() -> None:
    parser = argparse.ArgumentParser()
    add_release_argument(parser)

    with pytest.raises(SystemExit):
        parser.parse_args(["--release-type", "bad_release"])

