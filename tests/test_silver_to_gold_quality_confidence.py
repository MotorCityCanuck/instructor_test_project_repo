"""Tests for Silver-to-Gold Phase 8 quality-confidence builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.quality_confidence import (
    HIGH_CONFIDENCE,
    LOW_CONFIDENCE,
    MODERATE_CONFIDENCE,
    CRITICAL_CONFIDENCE,
    build_entity_data_quality_confidence_sql,
    calculate_weighted_confidence_score,
    publish_entity_data_quality_confidence,
    quality_confidence_band_for_score,
)
from napa_pipeline.silver_to_gold.quality_confidence_validation import (
    PHASE8_REQUIRED_SOURCE_COLUMNS,
    publish_phase8_quality_table,
    validate_phase8_source_contract,
)


def _quality_rules_config():
    return load_silver_to_gold_config("napa_5k").data["quality_rules"]


class _FakeField:
    def __init__(self, name: str):
        self.name = name


class _FakeSchema:
    def __init__(self, field_names):
        self.fields = [_FakeField(name) for name in field_names]


class _FakeTable:
    def __init__(self, *, field_names=None, row_count: int = 0):
        self.schema = _FakeSchema(field_names or [])
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count


class _FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping

    def asDict(self, recursive: bool = True):
        return dict(self._mapping)


class _FakeCollectResult:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return [_FakeRow(row) for row in self._rows]


class _FakeSpark:
    def __init__(self, tables, sql_rows_by_query=None):
        self._tables = tables
        self._sql_rows_by_query = sql_rows_by_query or {}

    def table(self, table_name: str):
        return self._tables[table_name]

    def sql(self, query: str):
        rows = self._sql_rows_by_query.get(query)
        if rows is None:
            raise RuntimeError(f"unexpected query: {query}")
        return _FakeCollectResult(rows)


def test_calculate_weighted_confidence_score_uses_configured_weights() -> None:
    score = calculate_weighted_confidence_score(
        {
            "identity_integrity": 100.0,
            "relationship_integrity": 50.0,
            "match_structure_integrity": 0.0,
        },
        {
            "identity_integrity": 0.5,
            "relationship_integrity": 0.3,
            "match_structure_integrity": 0.2,
        },
    )

    assert score == 65.0


def test_quality_confidence_band_for_score_uses_boundaries_and_critical_override() -> None:
    thresholds = {"high": 85, "moderate": 70, "low": 50}

    assert quality_confidence_band_for_score(90.0, thresholds) == HIGH_CONFIDENCE
    assert quality_confidence_band_for_score(75.0, thresholds) == MODERATE_CONFIDENCE
    assert quality_confidence_band_for_score(55.0, thresholds) == LOW_CONFIDENCE
    assert quality_confidence_band_for_score(40.0, thresholds) == CRITICAL_CONFIDENCE
    assert quality_confidence_band_for_score(95.0, thresholds, critical_issue_count=1) == CRITICAL_CONFIDENCE


def test_build_entity_data_quality_confidence_sql_contains_player_and_team_paths() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_entity_data_quality_confidence_sql(
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        quality_rules_config=_quality_rules_config(),
    )

    assert "player_components AS (" in sql
    assert "team_components AS (" in sql
    assert "UNION ALL" in sql
    assert "data_quality_confidence_score" in sql
    assert "quality_confidence_band" in sql


def test_validate_phase8_source_contract_checks_gold_and_silver_tables() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    tables = {}
    for logical_name, (layer, table_name, required_columns) in PHASE8_REQUIRED_SOURCE_COLUMNS.items():
        schema_name = environment.gold_schema if layer == "gold" else environment.silver_schema
        table_fqn = f"{environment.catalog}.{schema_name}.{table_name}"
        tables[table_fqn] = _FakeTable(field_names=required_columns)

    validated = validate_phase8_source_contract(_FakeSpark(tables), environment)

    assert set(validated) == set(PHASE8_REQUIRED_SOURCE_COLUMNS)


def test_publish_entity_data_quality_confidence_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    players_fqn = f"{environment.catalog}.{environment.silver_schema}.players"
    teams_fqn = f"{environment.catalog}.{environment.silver_schema}.teams"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.entity_data_quality_confidence"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.entity_data_quality_confidence"
    spark = _FakeSpark(
        {
            players_fqn: _FakeTable(row_count=4),
            teams_fqn: _FakeTable(row_count=3),
            target_fqn: _FakeTable(row_count=7),
        }
    )
    published = {}

    def _fake_publish_stage_to_gold_table(
        _spark,
        *,
        stage_table_fqn: str,
        target_table_fqn: str,
        stage_sql: str,
        validation_fn=None,
        count_fn=None,
    ):
        published["stage_table_fqn"] = stage_table_fqn
        published["target_table_fqn"] = target_table_fqn
        published["stage_sql"] = stage_sql
        return 7, 7

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.quality_confidence.publish_stage_to_gold_table",
        _fake_publish_stage_to_gold_table,
    )

    summary = publish_entity_data_quality_confidence(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        quality_rules_config=_quality_rules_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 7
    assert summary.output_row_count == 7
    assert "competition_player_matches" in published["stage_sql"]
    assert "competition_match_sides" in published["stage_sql"]


def test_publish_phase8_quality_table_returns_summary(monkeypatch) -> None:
    quality_summary = object()

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.quality_confidence_validation.publish_entity_data_quality_confidence",
        lambda *args, **kwargs: quality_summary,
    )

    summary = publish_phase8_quality_table(
        spark=None,
        environment=None,
        analysis_as_of_date=date(2026, 6, 30),
        quality_rules_config=_quality_rules_config(),
    )

    assert summary.entity_data_quality_confidence is quality_summary
