"""Tests for Silver-to-Gold analysis-date resolution."""

from datetime import date, datetime

import pytest

from napa_pipeline.silver_to_gold.time_controls import (
    AnalysisDateResolutionError,
    resolve_analysis_as_of_date,
)


def test_resolve_analysis_as_of_date_uses_max_valid_completed_match_date() -> None:
    resolved = resolve_analysis_as_of_date(
        [
            {"match_date": "2026-06-15", "completed_flag": True},
            {"match_date": date(2026, 6, 20), "completed_flag": True},
            {"match_date": datetime(2026, 6, 18, 9, 30, 0), "completed_flag": True},
            {"match_date": "2026-06-25", "completed_flag": False},
        ]
    )

    assert resolved == date(2026, 6, 20)


def test_resolve_analysis_as_of_date_returns_explicit_override() -> None:
    explicit = date(2026, 5, 31)

    resolved = resolve_analysis_as_of_date(
        [{"match_date": "2026-06-15", "completed_flag": True}],
        explicit_analysis_as_of_date=explicit,
    )

    assert resolved == explicit


def test_resolve_analysis_as_of_date_rejects_when_no_valid_completed_matches_exist() -> None:
    with pytest.raises(AnalysisDateResolutionError, match="Could not resolve analysis_as_of_date"):
        resolve_analysis_as_of_date(
            [
                {"match_date": "2026-06-15", "completed_flag": False},
                {"match_date": None, "completed_flag": True},
            ]
        )

