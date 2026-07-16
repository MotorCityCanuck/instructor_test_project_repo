"""Tests for Raw-to-Bronze script task CLI helpers."""

import argparse

import pytest

from napa_pipeline.raw_to_bronze.cli import (
    ALLOWED_RELEASE_TYPES,
    add_release_type_argument,
    release_type_to_release_name,
)


def test_release_type_to_release_name_maps_supported_values() -> None:
    assert release_type_to_release_name("5k") == "napa_5k"
    assert release_type_to_release_name("50k") == "napa_50k"
    assert release_type_to_release_name("250k") == "napa_250k"


def test_release_type_to_release_name_rejects_unsupported_value() -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="Unsupported release_type"):
        release_type_to_release_name("napa_5k")


def test_release_type_parser_accepts_only_short_release_types() -> None:
    parser = argparse.ArgumentParser()
    add_release_type_argument(parser)

    parsed = parser.parse_args(["--release-type", "5k"])

    assert parsed.release_type == "5k"
    assert ALLOWED_RELEASE_TYPES == ("5k", "50k", "250k")


def test_release_type_parser_exits_on_invalid_value() -> None:
    parser = argparse.ArgumentParser()
    add_release_type_argument(parser)

    with pytest.raises(SystemExit):
        parser.parse_args(["--release-type", "napa_5k"])
