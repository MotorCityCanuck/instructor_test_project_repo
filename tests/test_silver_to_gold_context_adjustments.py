"""Tests for governed Gold regional, age, and fatigue context features."""

from datetime import date

import pytest

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.context_adjustments import (
    build_team_context_adjustment_features_sql,
    context_adjustment_factor,
    geometric_team_factor,
    player_age_factor,
    player_fatigue_factor,
    player_regional_strength_factor,
)
from napa_pipeline.silver_to_gold.environment import resolve_release_environment


def _context_config():
    return load_silver_to_gold_config("napa_5k").data["context_adjustments"]


@pytest.mark.parametrize(
    ("age", "expected"),
    [(35, 1.0), (40, 0.9985), (45, 0.994), (50, 0.9865), (65, 0.946), (80, 0.94)],
)
def test_player_age_factor_uses_approved_quadratic_curve(age, expected) -> None:
    assert player_age_factor(age, _context_config()["age"]) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("load", "expected"),
    [(0.0, 1.0), (0.75, 1.0), (1.0, 0.9925), (1.5, 0.9775), (3.0, 0.95)],
)
def test_player_fatigue_factor_uses_approved_threshold_and_floor(load, expected) -> None:
    assert player_fatigue_factor(load, _context_config()["fatigue"]) == pytest.approx(expected)


def test_player_regional_factor_is_neutral_when_evidence_is_insufficient() -> None:
    regional = _context_config()["regional"]
    assert player_regional_strength_factor(
        effective_observations=99.5,
        regional_mean_residual=0.1,
        country_mean_residual=0.0,
        regional_config=regional,
    ) == 1.0


def test_player_regional_factor_is_centered_shrunk_and_bounded() -> None:
    regional = _context_config()["regional"]
    positive = player_regional_strength_factor(
        effective_observations=1000.0,
        regional_mean_residual=0.5,
        country_mean_residual=0.0,
        regional_config=regional,
    )
    negative = player_regional_strength_factor(
        effective_observations=1000.0,
        regional_mean_residual=-0.5,
        country_mean_residual=0.0,
        regional_config=regional,
    )
    assert positive == 1.02
    assert negative == 0.98


def test_team_factors_use_geometric_mean_and_composite_bounds() -> None:
    assert geometric_team_factor(1.0, 0.96) == pytest.approx((0.96) ** 0.5)
    assert context_adjustment_factor(
        team_regional_strength_factor=1.02,
        team_age_factor=0.94,
        team_fatigue_factor=0.95,
        composite_config=_context_config()["composite"],
    ) == pytest.approx(0.92)


def test_context_sql_uses_time_controls_and_deduplicates_player_matches() -> None:
    config = load_silver_to_gold_config("napa_5k")
    sql = build_team_context_adjustment_features_sql(
        resolve_release_environment(config),
        analysis_as_of_date=date(2025, 12, 31),
        context_config=_context_config(),
    )

    assert "DATE_SUB(DATE('2025-12-31'), 365)" in sql
    assert "CAST(match_date AS DATE) < DATE('2025-12-31')" in sql
    assert "PARTITION BY CAST(match_id AS STRING), CAST(player_id AS STRING)" in sql
    assert "SQRT" in sql
    assert "context_adjustment_factor" in sql
