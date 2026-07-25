"""Tests for Silver-to-Gold Phase 7 team and partnership feature builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.team_features import (
    build_partnership_effectiveness_sql,
    build_team_performance_features_sql,
    calculate_evidence_reliability_score,
    calculate_shrinkage_adjusted_win_rate,
    get_team_feature_registry,
    publish_partnership_effectiveness,
    publish_team_performance_features,
)
from napa_pipeline.silver_to_gold.team_features_validation import (
    PHASE7_REQUIRED_SOURCE_COLUMNS,
    publish_phase7_team_tables,
    validate_phase7_source_contract,
)


def _features_config():
    return load_silver_to_gold_config("napa_5k").data["features"]


def _evidence_windows_config():
    return load_silver_to_gold_config("napa_5k").data["evidence_windows"]


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


def test_calculate_shrinkage_adjusted_win_rate_uses_neutral_prior() -> None:
    adjusted = calculate_shrinkage_adjusted_win_rate(
        win_count=2,
        match_count=3,
        prior_matches=3,
    )

    assert round(adjusted or 0.0, 6) == round((2 + 1.5) / 6.0, 6)
    assert calculate_shrinkage_adjusted_win_rate(
        win_count=0,
        match_count=0,
        prior_matches=3,
    ) is None


def test_calculate_evidence_reliability_score_penalizes_non_attributable_rows() -> None:
    attributable = calculate_evidence_reliability_score(
        match_count=3,
        sufficient_match_count=3,
        attributable_flag=True,
    )
    unattributable = calculate_evidence_reliability_score(
        match_count=3,
        sufficient_match_count=3,
        attributable_flag=False,
    )

    assert attributable == 100.0
    assert unattributable == 60.0


def test_get_team_feature_registry_contains_phase7_entries() -> None:
    registry = get_team_feature_registry()

    assert len(registry) >= 6
    assert any(entry["feature_name"] == "shrinkage_adjusted_win_rate" for entry in registry)
    assert any(entry["feature_name"] == "synergy_proxy" for entry in registry)


def test_build_team_performance_features_sql_uses_windows_and_shrinkage_logic() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_team_performance_features_sql(
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        features_config=_features_config(),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert "CROSS JOIN windows" in sql
    assert "NTILE(4) OVER" in sql
    assert "shrinkage_adjusted_win_rate" in sql
    assert "resolved_match_teams" in sql
    assert "toLocalIterator" not in sql


def test_build_partnership_effectiveness_sql_uses_player_and_team_inputs() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_partnership_effectiveness_sql(
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert "COALESCE(CAST(rmt.resolved_team_id AS STRING), CAST(rmt.canonical_player_pair_key AS STRING))" in sql
    assert "player_performance_features" in sql
    assert "player_current_ratings" in sql
    assert "synergy_proxy" in sql


def test_validate_phase7_source_contract_checks_gold_and_silver_tables() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    tables = {}
    for logical_name, (layer, table_name, required_columns) in PHASE7_REQUIRED_SOURCE_COLUMNS.items():
        schema_name = environment.gold_schema if layer == "gold" else environment.silver_schema
        table_fqn = f"{environment.catalog}.{schema_name}.{table_name}"
        tables[table_fqn] = _FakeTable(field_names=required_columns)

    validated = validate_phase7_source_contract(_FakeSpark(tables), environment)

    assert set(validated) == set(PHASE7_REQUIRED_SOURCE_COLUMNS)


def test_publish_team_performance_features_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    teams_fqn = f"{environment.catalog}.{environment.silver_schema}.teams"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.team_performance_features"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.team_performance_features"
    spark = _FakeSpark(
        {
            teams_fqn: _FakeTable(row_count=5),
            target_fqn: _FakeTable(row_count=20),
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
        return 20, 20

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_features.publish_stage_to_gold_table",
        _fake_publish_stage_to_gold_table,
    )

    summary = publish_team_performance_features(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        features_config=_features_config(),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 20
    assert summary.output_row_count == 20
    assert "competition_match_sides" in published["stage_sql"]


def test_publish_partnership_effectiveness_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.partnership_effectiveness"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.partnership_effectiveness"
    source_count_query = f"""
SELECT COUNT(*) AS partnership_count
FROM (
    SELECT DISTINCT
        COALESCE(CAST(resolved_team_id AS STRING), CAST(canonical_player_pair_key AS STRING)) AS partnership_key
    FROM {environment.catalog}.{environment.gold_schema}.resolved_match_teams
    WHERE canonical_player_pair_key IS NOT NULL
      AND match_date IS NOT NULL
      AND CAST(match_date AS DATE) <= DATE('2026-06-30')
)
""".strip()
    spark = _FakeSpark(
        {
            target_fqn: _FakeTable(row_count=8),
        },
        sql_rows_by_query={source_count_query: [{"partnership_count": 8}]},
    )

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_features.publish_stage_to_gold_table",
        lambda *args, **kwargs: (8, 8),
    )

    summary = publish_partnership_effectiveness(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 8
    assert summary.output_row_count == 8


def test_publish_phase7_team_tables_returns_two_summaries(monkeypatch) -> None:
    team_summary = object()
    partnership_summary = object()

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_features_validation.publish_team_performance_features",
        lambda *args, **kwargs: team_summary,
    )
    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_features_validation.publish_partnership_effectiveness",
        lambda *args, **kwargs: partnership_summary,
    )

    summary = publish_phase7_team_tables(
        spark=None,
        environment=None,
        analysis_as_of_date=date(2026, 6, 30),
        features_config=_features_config(),
        evidence_windows_config=_evidence_windows_config(),
    )

    assert summary.team_performance_features is team_summary
    assert summary.partnership_effectiveness is partnership_summary
