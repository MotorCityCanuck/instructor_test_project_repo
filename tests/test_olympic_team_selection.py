"""Tests for post-Gold Olympic team-set selection validation."""

import pytest

from napa_pipeline.silver_to_gold.olympic_team_selection import (
    OlympicTeamSelectionError,
    REQUIRED_GROUPS,
    build_olympic_team_selections_sql,
    parse_team_set_count,
    validate_gold_source_contract,
    validate_preselection_counts,
    validate_selection_rows,
)
from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment


@pytest.mark.parametrize("value, expected", [("1", 1), ("4", 4), (10, 10)])
def test_parse_team_set_count_accepts_positive_integers(value, expected) -> None:
    assert parse_team_set_count(value) == expected


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "one", True])
def test_parse_team_set_count_rejects_invalid_values(value) -> None:
    with pytest.raises(OlympicTeamSelectionError, match="positive integer"):
        parse_team_set_count(value)


def _valid_rows(team_set_count: int = 1):
    rows = []
    for country_code, division in REQUIRED_GROUPS:
        for rank in range(1, team_set_count + 1):
            team_id = f"{country_code}-{division}-{rank}"
            rows.append(
                {
                    "country_code": country_code,
                    "division": division,
                    "team_number": team_id,
                    "source_team_id": team_id,
                    "selection_rank": rank,
                }
            )
    return rows


def test_validate_selection_rows_accepts_complete_team_sets() -> None:
    validate_selection_rows(_valid_rows(4), team_set_count=4)


def test_validate_selection_rows_rejects_synthetic_or_untraceable_team() -> None:
    rows = _valid_rows()
    rows[0]["team_number"] = "invented-team"
    with pytest.raises(OlympicTeamSelectionError, match="traces to its Gold scorecard"):
        validate_selection_rows(rows, team_set_count=1)


def test_validate_selection_rows_rejects_duplicate_team_within_group() -> None:
    rows = _valid_rows(2)
    rows[1]["team_number"] = rows[0]["team_number"]
    rows[1]["source_team_id"] = rows[0]["team_number"]
    with pytest.raises(OlympicTeamSelectionError, match="duplicate team numbers"):
        validate_selection_rows(rows, team_set_count=2)


def test_validate_preselection_counts_reports_short_group() -> None:
    counts = {group: 4 for group in REQUIRED_GROUPS}
    counts[("CAN", "MIXED")] = 3
    with pytest.raises(OlympicTeamSelectionError, match="napa_5k / CAN / MIXED"):
        validate_preselection_counts(counts, dataset_source="napa_5k", team_set_count=4)


class _FakeField:
    def __init__(self, name: str):
        self.name = name


class _FakeTable:
    def __init__(self, field_names: list[str]):
        self.schema = type("_Schema", (), {"fields": [_FakeField(name) for name in field_names]})()


class _FakeSpark:
    def __init__(self, table):
        self._table = table

    def table(self, _table_name: str):
        return self._table


def test_gold_source_contract_rejects_missing_governed_adjustments() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    spark = _FakeSpark(_FakeTable(["team_id", "final_team_selection_score"]))

    with pytest.raises(OlympicTeamSelectionError, match="regional_adjustment"):
        validate_gold_source_contract(spark, environment)


def test_selection_sql_uses_governed_score_and_deterministic_tie_breakers() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    sql = build_olympic_team_selections_sql(
        environment,
        dataset_source="napa_5k",
        team_set_count=4,
        selection_run_id="run-1",
        scoring_scenario="BALANCED",
    )

    assert "team_selection_scorecards" in sql
    assert "final_team_selection_score AS selection_score" in sql
    assert "regional_adjustment" in sql
    assert "age_adjustment" in sql
    assert "fatigue_adjustment" in sql
    assert "confidence_metric DESC NULLS LAST" in sql
    assert "team_id ASC" in sql
    assert "selection_rank <= 4" in sql
