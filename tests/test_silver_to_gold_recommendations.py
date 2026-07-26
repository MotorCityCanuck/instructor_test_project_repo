"""Tests for Silver-to-Gold Phase 12 recommendation builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.recommendations import (
    ALTERNATE_STATUS,
    PRIMARY_STATUS,
    RANKED_CANDIDATE_STATUS,
    WATCHLIST_STATUS,
    build_olympic_team_recommendations,
    build_olympic_team_recommendations_sql,
)
from napa_pipeline.silver_to_gold.recommendations_validation import (
    PHASE12_REQUIRED_SOURCE_COLUMNS,
    publish_phase12_recommendation_tables,
    validate_phase12_source_contract,
)


def _eligibility_config():
    return load_silver_to_gold_config("napa_5k").data["eligibility"]


def _environment():
    config = load_silver_to_gold_config("napa_5k")
    return resolve_release_environment(config)


class _FakeField:
    def __init__(self, name: str):
        self.name = name


class _FakeSchema:
    def __init__(self, field_names):
        self.fields = [_FakeField(name) for name in field_names]


class _FakeTable:
    def __init__(self, *, field_names=None):
        self.schema = _FakeSchema(field_names or [])


class _FakeSpark:
    def __init__(self, tables):
        self._tables = tables

    def table(self, table_name: str):
        return self._tables[table_name]


def _sample_scorecard_rows(total_rows: int = 7) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(total_rows):
        rows.append(
            {
                "team_id": f"team-{index + 1}",
                "scoring_scenario": "BALANCED",
                "analysis_as_of_date": date(2025, 12, 31),
                "team_category": "MENS",
                "country_code": "USA",
                "player_one_id": f"player-{index + 1}a",
                "player_two_id": f"player-{index + 1}b",
                "final_team_selection_score": 95.0 - index,
                "combined_team_confidence": 85.0 - index,
                "top_strengths": "partnership,confidence",
                "top_risks": "limited_evidence" if index >= 5 else None,
                "ranking_rationale": f"base-rationale-{index + 1}",
                "eligibility_status": "ELIGIBLE",
            }
        )
    return rows


def test_build_olympic_team_recommendations_assigns_all_status_bands() -> None:
    rows = build_olympic_team_recommendations(
        team_selection_scorecard_rows=_sample_scorecard_rows(),
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        release_name="napa_5k",
        release_role="development",
        authoritative_recommendation_flag=False,
        methodology_version="1.0.0",
        eligibility_config=_eligibility_config(),
    )

    assert len(rows) == 7
    assert rows[0]["recommendation_status"] == PRIMARY_STATUS
    assert rows[1]["recommendation_status"] == ALTERNATE_STATUS
    assert rows[2]["recommendation_status"] == ALTERNATE_STATUS
    assert rows[3]["recommendation_status"] == WATCHLIST_STATUS
    assert rows[5]["recommendation_status"] == WATCHLIST_STATUS
    assert rows[6]["recommendation_status"] == RANKED_CANDIDATE_STATUS
    assert rows[0]["candidate_rank"] == 1
    assert rows[6]["candidate_rank"] == 7
    assert rows[0]["closest_alternative_team_id"] == "team-2"
    assert rows[1]["closest_alternative_team_id"] == "team-1"
    assert rows[1]["score_gap_to_primary"] == 1.0
    assert rows[1]["score_gap_to_previous"] == 1.0
    assert rows[0]["authoritative_recommendation_flag"] is False
    assert rows[0]["recommendation_shortfall_flag"] is False


def test_build_olympic_team_recommendations_marks_shortfall_when_pool_is_small() -> None:
    rows = build_olympic_team_recommendations(
        team_selection_scorecard_rows=_sample_scorecard_rows(total_rows=2),
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        release_name="napa_5k",
        release_role="development",
        authoritative_recommendation_flag=False,
        methodology_version="1.0.0",
        eligibility_config=_eligibility_config(),
    )

    assert len(rows) == 2
    assert rows[0]["recommendation_shortfall_flag"] is True
    assert rows[0]["recommendation_shortfall_reason"] == (
        "AVAILABLE_CANDIDATES_BELOW_CONFIGURED_TARGET:2/6"
    )


def test_build_olympic_team_recommendations_sql_contains_release_and_rank_fields() -> None:
    sql = build_olympic_team_recommendations_sql(
        _environment(),
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        release_name="napa_5k",
        release_role="development",
        authoritative_recommendation_flag=False,
        methodology_version="1.0.0",
        eligibility_config=_eligibility_config(),
    )

    assert "RANKED_CANDIDATE" in sql
    assert "authoritative_recommendation_flag" in sql
    assert "recommendation_shortfall_flag" in sql
    assert "constraint_applied_flag" in sql


def test_validate_phase12_source_contract_accepts_required_columns() -> None:
    environment = _environment()
    table_fqn = (
        f"{environment.catalog}.{environment.gold_schema}.team_selection_scorecards"
    )
    spark = _FakeSpark(
        {
            table_fqn: _FakeTable(
                field_names=PHASE12_REQUIRED_SOURCE_COLUMNS["team_selection_scorecards"]
            )
        }
    )

    validated_columns = validate_phase12_source_contract(spark, environment)

    assert validated_columns["team_selection_scorecards"] == PHASE12_REQUIRED_SOURCE_COLUMNS[
        "team_selection_scorecards"
    ]


def test_publish_phase12_recommendation_tables_delegates_to_builder(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_publish(
        spark,
        environment,
        *,
        analysis_as_of_date,
        scoring_scenario,
        release_name,
        release_role,
        authoritative_recommendation_flag,
        pipeline_version,
        eligibility_config,
    ):
        captured.update(
            {
                "analysis_as_of_date": analysis_as_of_date,
                "scoring_scenario": scoring_scenario,
                "release_name": release_name,
                "release_role": release_role,
                "authoritative_recommendation_flag": authoritative_recommendation_flag,
                "pipeline_version": pipeline_version,
                "eligibility_config": eligibility_config,
            }
        )
        return "summary"

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.recommendations_validation.publish_phase12_recommendation_table",
        _fake_publish,
    )

    summary = publish_phase12_recommendation_tables(
        spark=object(),
        environment=_environment(),
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        release_name="napa_5k",
        release_role="development",
        authoritative_recommendation_flag=False,
        pipeline_version="1.0.0",
        eligibility_config=_eligibility_config(),
    )

    assert summary == "summary"
    assert captured["pipeline_version"] == "1.0.0"
