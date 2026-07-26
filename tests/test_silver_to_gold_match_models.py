"""Tests for Silver-to-Gold Phase 9 match modeling builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.match_models import (
    BASELINE_ALGORITHM,
    build_match_model_metric_records,
    build_match_outcome_predictions_sql,
    build_match_outcome_training_set_sql,
    calculate_brier_score,
    calculate_calibration_band_metrics,
    calculate_log_loss,
    calculate_roc_auc,
    publish_match_model_metrics,
    publish_match_outcome_predictions,
    publish_match_outcome_training_set,
)
from napa_pipeline.silver_to_gold.match_models_validation import (
    PHASE9_REQUIRED_SOURCE_COLUMNS,
    publish_phase9_tables,
    validate_phase9_source_contract,
)


def _models_config():
    return load_silver_to_gold_config("napa_5k").data["models"]


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


def test_build_match_outcome_training_set_sql_uses_deterministic_team_assignment() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_match_outcome_training_set_sql(
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        models_config=_models_config(),
    )

    assert "side_one.team_number = 1" in sql
    assert "side_two.team_number = 2" in sql
    assert "rating_expected_probability" in sql
    assert "split_name" in sql
    assert "POWER(" in sql


def test_build_match_outcome_predictions_sql_uses_baseline_probability() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_match_outcome_predictions_sql(
        environment,
        model_run_id="model-run-1",
        model_name="analytical_rating_probability",
        model_version="baseline_v1",
    )

    assert "model_predicted_probability" in sql
    assert "rating_expected_probability" in sql
    assert "prediction_explanation" in sql
    assert "analytical_rating_probability" in sql


def test_calculate_metric_helpers_return_expected_values() -> None:
    probabilities = [0.9, 0.7, 0.4, 0.2]
    outcomes = [1, 1, 0, 0]

    assert round(calculate_brier_score(probabilities, outcomes) or 0.0, 6) == 0.075
    assert round(calculate_log_loss(probabilities, outcomes) or 0.0, 6) == round(
        -(
            __import__("math").log(0.9)
            + __import__("math").log(0.7)
            + __import__("math").log(0.6)
            + __import__("math").log(0.8)
        )
        / 4.0,
        6,
    )
    assert calculate_roc_auc(probabilities, outcomes) == 1.0


def test_calculate_calibration_band_metrics_returns_named_bands() -> None:
    metrics = calculate_calibration_band_metrics(
        [0.1, 0.3, 0.55, 0.75, 0.95],
        [0, 0, 1, 1, 1],
    )

    assert len(metrics) == 5
    assert metrics[0][0] == "calibration_gap_band_00_20"
    assert metrics[-1][0] == "calibration_gap_band_80_100"


def test_validate_phase9_source_contract_checks_required_gold_tables() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    tables = {}
    for logical_name, (table_name, required_columns) in PHASE9_REQUIRED_SOURCE_COLUMNS.items():
        table_fqn = f"{environment.catalog}.{environment.gold_schema}.{table_name}"
        tables[table_fqn] = _FakeTable(field_names=required_columns)

    validated = validate_phase9_source_contract(_FakeSpark(tables), environment)

    assert set(validated) == set(PHASE9_REQUIRED_SOURCE_COLUMNS)


def test_build_match_model_metric_records_returns_split_metrics() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    predictions_fqn = f"{environment.catalog}.{environment.gold_schema}.match_outcome_predictions"
    query = f"""
SELECT
    split_name,
    model_predicted_probability,
    team_a_win_flag
FROM {predictions_fqn}
""".strip()
    spark = _FakeSpark(
        {predictions_fqn: _FakeTable(row_count=4)},
        sql_rows_by_query={
            query: [
                {"split_name": "train", "model_predicted_probability": 0.8, "team_a_win_flag": True},
                {"split_name": "train", "model_predicted_probability": 0.3, "team_a_win_flag": False},
                {"split_name": "validation", "model_predicted_probability": 0.7, "team_a_win_flag": True},
                {"split_name": "validation", "model_predicted_probability": 0.4, "team_a_win_flag": False},
            ]
        },
    )

    records = build_match_model_metric_records(
        spark,
        environment,
        model_run_id="model-run-1",
        model_name="analytical_rating_probability",
        model_version="baseline_v1",
        feature_definition_version="cfg-hash-1",
    )

    assert any(record["metric_name"] == "accuracy" for record in records)
    assert any(record["metric_name"] == "roc_auc" for record in records)
    assert any(record["metric_name"] == "calibration_gap_band_40_60" for record in records)
    assert all(record["algorithm"] == BASELINE_ALGORITHM for record in records)


def test_publish_match_outcome_training_set_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.match_outcome_training_set"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.match_outcome_training_set"
    source_count_query = f"""
SELECT COUNT(*) AS match_count
FROM (
    SELECT DISTINCT CAST(match_id AS STRING) AS match_id
    FROM {environment.catalog}.{environment.gold_schema}.competition_match_sides
    WHERE CAST(match_date AS DATE) IS NOT NULL
      AND CAST(match_date AS DATE) <= DATE('2026-06-30')
      AND CAST(winning_team_number AS INT) IN (1, 2)
      AND COALESCE(CAST(completed_flag AS BOOLEAN), FALSE) = TRUE
)
""".strip()
    spark = _FakeSpark(
        {target_fqn: _FakeTable(row_count=8)},
        sql_rows_by_query={source_count_query: [{"match_count": 8}]},
    )

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.match_models.publish_stage_to_gold_table",
        lambda *args, **kwargs: (8, 8),
    )

    summary = publish_match_outcome_training_set(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        models_config=_models_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 8
    assert summary.output_row_count == 8


def test_publish_match_outcome_predictions_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    training_fqn = f"{environment.catalog}.{environment.gold_schema}.match_outcome_training_set"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.match_outcome_predictions"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.match_outcome_predictions"
    spark = _FakeSpark(
        {
            training_fqn: _FakeTable(row_count=8),
            target_fqn: _FakeTable(row_count=8),
        }
    )

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.match_models.publish_stage_to_gold_table",
        lambda *args, **kwargs: (8, 8),
    )

    summary = publish_match_outcome_predictions(
        spark,
        environment,
        model_run_id="model-run-1",
        model_name="analytical_rating_probability",
        model_version="baseline_v1",
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 8
    assert summary.output_row_count == 8


def test_publish_match_model_metrics_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    predictions_fqn = f"{environment.catalog}.{environment.gold_schema}.match_outcome_predictions"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.match_model_metrics"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.match_model_metrics"
    query = f"""
SELECT
    split_name,
    model_predicted_probability,
    team_a_win_flag
FROM {predictions_fqn}
""".strip()
    spark = _FakeSpark(
        {
            target_fqn: _FakeTable(row_count=24),
        },
        sql_rows_by_query={
            query: [
                {"split_name": "train", "model_predicted_probability": 0.8, "team_a_win_flag": True},
                {"split_name": "validation", "model_predicted_probability": 0.3, "team_a_win_flag": False},
            ]
        },
    )

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.match_models.publish_stage_records_to_gold_table",
        lambda *args, **kwargs: (24, 24),
    )

    summary = publish_match_model_metrics(
        spark,
        environment,
        model_run_id="model-run-1",
        model_name="analytical_rating_probability",
        model_version="baseline_v1",
        feature_definition_version="cfg-hash-1",
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == len(summary.metric_records)
    assert summary.output_row_count == 24


def test_publish_phase9_tables_returns_three_summaries(monkeypatch) -> None:
    training_summary = object()
    predictions_summary = object()
    metrics_summary = object()

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.match_models_validation.publish_phase9_modeling_tables",
        lambda *args, **kwargs: type(
            "_Summary",
            (),
            {
                "training_set": training_summary,
                "predictions": predictions_summary,
                "metrics": metrics_summary,
            },
        )(),
    )

    summary = publish_phase9_tables(
        spark=None,
        environment=None,
        analysis_as_of_date=date(2026, 6, 30),
        models_config=_models_config(),
        model_run_id="model-run-1",
        model_name="analytical_rating_probability",
        model_version="baseline_v1",
        feature_definition_version="cfg-hash-1",
    )

    assert summary.training_set is training_summary
    assert summary.predictions is predictions_summary
    assert summary.metrics is metrics_summary
