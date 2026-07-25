"""Tests for the Silver-to-Gold Phase 3 competition validation helpers."""

from datetime import date

import pytest

from napa_pipeline.silver_to_gold.competition import (
    CompetitionMatchSidesPublicationSummary,
    CompetitionPlayerMatchesPublicationSummary,
)
from napa_pipeline.silver_to_gold.competition_validation import (
    PHASE3_REQUIRED_SOURCE_COLUMNS,
    Phase3SourceContractError,
    publish_phase3_competition_foundation,
    validate_phase3_source_contract,
)
from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment


class _FakeField:
    def __init__(self, name: str):
        self.name = name


class _FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class _FakeTable:
    def __init__(self, column_names):
        self.schema = _FakeSchema([_FakeField(name) for name in column_names])


class _FakeSpark:
    def __init__(self, columns_by_table):
        self._columns_by_table = columns_by_table
        self.requested_tables: list[str] = []

    def table(self, table_name: str):
        self.requested_tables.append(table_name)
        return _FakeTable(self._columns_by_table[table_name])


def _environment():
    config = load_silver_to_gold_config("napa_5k")
    return resolve_release_environment(config)


def test_validate_phase3_source_contract_accepts_required_columns() -> None:
    environment = _environment()
    spark = _FakeSpark(
        {
            f"{environment.catalog}.{environment.silver_schema}.{table_name}": columns
            for table_name, columns in PHASE3_REQUIRED_SOURCE_COLUMNS.items()
        }
    )

    validated = validate_phase3_source_contract(spark, environment)

    assert validated == PHASE3_REQUIRED_SOURCE_COLUMNS
    assert len(spark.requested_tables) == len(PHASE3_REQUIRED_SOURCE_COLUMNS)


def test_validate_phase3_source_contract_raises_clear_error_for_missing_column() -> None:
    environment = _environment()
    columns_by_table = {
        f"{environment.catalog}.{environment.silver_schema}.{table_name}": columns
        for table_name, columns in PHASE3_REQUIRED_SOURCE_COLUMNS.items()
    }
    matches_fqn = f"{environment.catalog}.{environment.silver_schema}.matches"
    columns_by_table[matches_fqn] = tuple(
        column for column in PHASE3_REQUIRED_SOURCE_COLUMNS["matches"] if column != "completed_flag"
    )
    spark = _FakeSpark(columns_by_table)

    with pytest.raises(Phase3SourceContractError, match="missing columns completed_flag"):
        validate_phase3_source_contract(spark, environment)


def test_publish_phase3_competition_foundation_calls_both_publishers(monkeypatch) -> None:
    environment = _environment()
    spark = object()
    calls: list[str] = []

    def _fake_publish_competition_match_sides(_spark, _environment, *, analysis_as_of_date):
        calls.append(f"match_sides:{analysis_as_of_date.isoformat()}")
        return CompetitionMatchSidesPublicationSummary(
            target_table_fqn="workspace.instructor_5k_gold.competition_match_sides",
            stage_table_fqn="workspace.instructor_5k_gold_stage.competition_match_sides",
            input_row_count=100,
            output_row_count=92,
        )

    def _fake_publish_competition_player_matches(_spark, _environment):
        calls.append("player_matches")
        return CompetitionPlayerMatchesPublicationSummary(
            target_table_fqn="workspace.instructor_5k_gold.competition_player_matches",
            stage_table_fqn="workspace.instructor_5k_gold_stage.competition_player_matches",
            input_row_count=92,
            output_row_count=184,
        )

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.competition_validation.publish_competition_match_sides",
        _fake_publish_competition_match_sides,
    )
    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.competition_validation.publish_competition_player_matches",
        _fake_publish_competition_player_matches,
    )

    summary = publish_phase3_competition_foundation(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
    )

    assert calls == ["match_sides:2026-06-30", "player_matches"]
    assert summary.competition_match_sides.output_row_count == 92
    assert summary.competition_player_matches.output_row_count == 184
