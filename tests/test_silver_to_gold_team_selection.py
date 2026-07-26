"""Tests for Silver-to-Gold Phase 11 team selection builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.team_selection import (
    build_olympic_team_candidates_sql,
    build_olympic_team_candidates,
    build_team_selection_scorecards_sql,
    build_team_selection_scorecards,
    publish_olympic_team_candidates,
    publish_olympic_team_candidates_from_sql,
    publish_phase11_team_tables,
    publish_team_selection_scorecards,
    publish_team_selection_scorecards_from_sql,
)
from napa_pipeline.silver_to_gold.team_selection_validation import (
    PHASE11_REQUIRED_SOURCE_COLUMNS,
    publish_phase11_selection_tables,
    validate_phase11_source_contract,
)


def _scorecards_config():
    return load_silver_to_gold_config("napa_5k").data["scorecards"]


def _eligibility_config():
    return load_silver_to_gold_config("napa_5k").data["eligibility"]


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

    def toLocalIterator(self):
        return iter(())


class _FakeSpark:
    def __init__(self, tables):
        self._tables = tables

    def table(self, table_name: str):
        return self._tables[table_name]

    def sql(self, _query: str):
        raise RuntimeError("sql should not be called in these tests")


def _sample_team_selection_inputs():
    teams = [
        {
            "team_id": "team-1",
            "team_category": "MENS",
            "country_code": "USA",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2025-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-2",
            "team_category": "MENS",
            "country_code": "USA",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2025-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-3",
            "team_category": "MENS",
            "country_code": "USA",
            "team_status": "INACTIVE",
            "active_flag": False,
            "formation_date": "2025-01-01",
            "dissolution_date": "2025-12-31",
        },
    ]
    memberships = [
        {"team_id": "team-1", "player_id": "player-1", "current_membership_flag": True, "membership_overlap_flag": False},
        {"team_id": "team-1", "player_id": "player-2", "current_membership_flag": True, "membership_overlap_flag": False},
        {"team_id": "team-2", "player_id": "player-3", "current_membership_flag": True, "membership_overlap_flag": False},
        {"team_id": "team-3", "player_id": "player-1", "current_membership_flag": True, "membership_overlap_flag": False},
        {"team_id": "team-3", "player_id": "player-2", "current_membership_flag": True, "membership_overlap_flag": False},
    ]
    team_features = [
        {
            "team_id": "team-1",
            "evidence_window": "career",
            "team_category": "MENS",
            "country_code": "USA",
            "candidate_attribution_allowed_flag": True,
            "shrinkage_adjusted_win_rate": 0.72,
            "recent_form_win_pct": 0.75,
            "performance_above_expectation": 0.09,
            "consistency_score": 81.0,
            "partnership_duration_days": 210,
            "evidence_reliability_score": 88.0,
            "feature_evidence_status": "SUFFICIENT",
        },
        {
            "team_id": "team-2",
            "evidence_window": "career",
            "team_category": "MENS",
            "country_code": "USA",
            "candidate_attribution_allowed_flag": False,
            "shrinkage_adjusted_win_rate": 0.61,
            "recent_form_win_pct": 0.6,
            "performance_above_expectation": 0.02,
            "consistency_score": 68.0,
            "partnership_duration_days": 90,
            "evidence_reliability_score": 52.0,
            "feature_evidence_status": "LIMITED",
        },
        {
            "team_id": "team-3",
            "evidence_window": "career",
            "team_category": "MENS",
            "country_code": "USA",
            "candidate_attribution_allowed_flag": True,
            "shrinkage_adjusted_win_rate": 0.5,
            "recent_form_win_pct": 0.5,
            "performance_above_expectation": 0.0,
            "consistency_score": 50.0,
            "partnership_duration_days": 60,
            "evidence_reliability_score": 40.0,
            "feature_evidence_status": "LIMITED",
        },
    ]
    partnerships = [
        {
            "partnership_key": "team-1",
            "team_id": "team-1",
            "player_one_id": "player-1",
            "player_two_id": "player-2",
            "team_adjusted_win_rate": 0.74,
            "synergy_proxy": 0.11,
            "partnership_duration_days": 240,
            "evidence_reliability_score": 90.0,
            "feature_evidence_status": "SUFFICIENT",
            "candidate_attribution_allowed_flag": True,
        },
        {
            "partnership_key": "team-2",
            "team_id": "team-2",
            "player_one_id": "player-3",
            "player_two_id": None,
            "team_adjusted_win_rate": 0.55,
            "synergy_proxy": 0.01,
            "partnership_duration_days": 90,
            "evidence_reliability_score": 45.0,
            "feature_evidence_status": "LIMITED",
            "candidate_attribution_allowed_flag": False,
        },
        {
            "partnership_key": "team-3",
            "team_id": "team-3",
            "player_one_id": "player-1",
            "player_two_id": "player-2",
            "team_adjusted_win_rate": 0.51,
            "synergy_proxy": -0.01,
            "partnership_duration_days": 75,
            "evidence_reliability_score": 42.0,
            "feature_evidence_status": "LIMITED",
            "candidate_attribution_allowed_flag": True,
        },
    ]
    player_scorecards = [
        {
            "player_id": "player-1",
            "scoring_scenario": "BALANCED",
            "display_name": "Player One",
            "country_code": "USA",
            "confidence_adjusted_player_score": 92.0,
            "combined_confidence_score": 88.0,
            "evidence_band": "HIGH",
        },
        {
            "player_id": "player-2",
            "scoring_scenario": "BALANCED",
            "display_name": "Player Two",
            "country_code": "USA",
            "confidence_adjusted_player_score": 84.0,
            "combined_confidence_score": 80.0,
            "evidence_band": "HIGH",
        },
        {
            "player_id": "player-3",
            "scoring_scenario": "BALANCED",
            "display_name": "Player Three",
            "country_code": "USA",
            "confidence_adjusted_player_score": 70.0,
            "combined_confidence_score": 62.0,
            "evidence_band": "MODERATE",
        },
    ]
    quality = [
        {
            "entity_type": "TEAM",
            "entity_id": "team-1",
            "data_quality_confidence_score": 91.0,
            "quality_confidence_band": "HIGH",
            "material_limitation_text": None,
        },
        {
            "entity_type": "TEAM",
            "entity_id": "team-2",
            "data_quality_confidence_score": 58.0,
            "quality_confidence_band": "LOW",
            "material_limitation_text": "limited resolved history",
        },
        {
            "entity_type": "TEAM",
            "entity_id": "team-3",
            "data_quality_confidence_score": 55.0,
            "quality_confidence_band": "LOW",
            "material_limitation_text": "inactive team",
        },
    ]
    resolved = [
        {"match_id": "m1", "match_date": "2025-06-01", "resolved_team_id": "team-1", "team_resolution_confidence": 95.0},
        {"match_id": "m2", "match_date": "2025-07-01", "resolved_team_id": "team-1", "team_resolution_confidence": 90.0},
        {"match_id": "m3", "match_date": "2025-06-15", "resolved_team_id": "team-2", "team_resolution_confidence": 65.0},
    ]
    predictions = [
        {
            "match_id": "pm1",
            "match_date": "2025-06-01",
            "team_a_team_id": "team-1",
            "team_b_team_id": "team-2",
            "model_predicted_probability": 0.72,
        },
        {
            "match_id": "pm2",
            "match_date": "2025-06-10",
            "team_a_team_id": "team-3",
            "team_b_team_id": "team-1",
            "model_predicted_probability": 0.30,
        },
    ]
    return teams, memberships, team_features, partnerships, player_scorecards, quality, resolved, predictions


def test_build_team_selection_scorecards_excludes_inactive_teams_and_classifies_hard_failures() -> None:
    (
        teams,
        memberships,
        team_features,
        partnerships,
        player_scorecards,
        quality,
        resolved,
        predictions,
    ) = _sample_team_selection_inputs()

    rows = build_team_selection_scorecards(
        teams_rows=teams,
        team_memberships_rows=memberships,
        team_performance_features_rows=team_features,
        partnership_effectiveness_rows=partnerships,
        player_scorecard_rows=player_scorecards,
        entity_data_quality_confidence_rows=quality,
        resolved_match_teams_rows=resolved,
        match_outcome_predictions_rows=predictions,
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    assert len(rows) == 2
    eligible_row = next(row for row in rows if row["team_id"] == "team-1")
    ineligible_row = next(row for row in rows if row["team_id"] == "team-2")

    assert eligible_row["eligibility_status"] == "ELIGIBLE"
    assert eligible_row["final_team_selection_score"] is not None
    assert eligible_row["candidate_attribution_allowed_flag"] is True
    assert ineligible_row["eligibility_status"] == "INELIGIBLE"
    assert "INVALID_MEMBERSHIP_COUNT" in str(ineligible_row["eligibility_reason_codes"])
    assert {row["team_id"] for row in rows} == {"team-1", "team-2"}


def test_build_team_selection_scorecards_uses_membership_dates_not_only_current_flag() -> None:
    (
        teams,
        memberships,
        team_features,
        partnerships,
        player_scorecards,
        quality,
        resolved,
        predictions,
    ) = _sample_team_selection_inputs()
    for membership in memberships:
        membership["membership_start_date"] = "2025-01-01"
        membership["membership_end_date"] = None
        membership["current_membership_flag"] = False

    rows = build_team_selection_scorecards(
        teams_rows=teams,
        team_memberships_rows=memberships,
        team_performance_features_rows=team_features,
        partnership_effectiveness_rows=partnerships,
        player_scorecard_rows=player_scorecards,
        entity_data_quality_confidence_rows=quality,
        resolved_match_teams_rows=resolved,
        match_outcome_predictions_rows=predictions,
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    eligible_row = next(row for row in rows if row["team_id"] == "team-1")
    assert eligible_row["current_member_count"] == 2
    assert eligible_row["eligibility_status"] == "ELIGIBLE"


def test_build_olympic_team_candidates_ranks_only_eligible_rows() -> None:
    (
        teams,
        memberships,
        team_features,
        partnerships,
        player_scorecards,
        quality,
        resolved,
        predictions,
    ) = _sample_team_selection_inputs()

    scorecards = build_team_selection_scorecards(
        teams_rows=teams,
        team_memberships_rows=memberships,
        team_performance_features_rows=team_features,
        partnership_effectiveness_rows=partnerships,
        player_scorecard_rows=player_scorecards,
        entity_data_quality_confidence_rows=quality,
        resolved_match_teams_rows=resolved,
        match_outcome_predictions_rows=predictions,
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    candidates = build_olympic_team_candidates(
        team_selection_scorecard_rows=scorecards,
        scoring_scenario="BALANCED",
        eligibility_config=_eligibility_config(),
    )

    assert len(candidates) == 1
    assert candidates[0]["team_id"] == "team-1"
    assert candidates[0]["candidate_rank"] == 1
    assert candidates[0]["recommendation_tier"] == "PRIMARY"


def test_build_olympic_team_candidates_includes_review_required_candidate_capable_rows() -> None:
    candidates = build_olympic_team_candidates(
        team_selection_scorecard_rows=(
            {
                "country_code": "USA",
                "team_category": "MENS",
                "team_id": "team-review",
                "scoring_scenario": "BALANCED",
                "analysis_as_of_date": date(2025, 12, 31),
                "eligible_team_flag": True,
                "eligibility_status": "REVIEW_REQUIRED",
                "final_team_selection_score": 72.0,
                "combined_team_confidence": 60.0,
            },
            {
                "country_code": "USA",
                "team_category": "MENS",
                "team_id": "team-hard-fail",
                "scoring_scenario": "BALANCED",
                "analysis_as_of_date": date(2025, 12, 31),
                "eligible_team_flag": False,
                "eligibility_status": "INELIGIBLE",
                "final_team_selection_score": 90.0,
                "combined_team_confidence": 90.0,
            },
        ),
        scoring_scenario="BALANCED",
        eligibility_config=_eligibility_config(),
    )

    assert len(candidates) == 1
    assert candidates[0]["team_id"] == "team-review"


def test_validate_phase11_source_contract_checks_gold_and_silver_tables() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    tables = {}
    for logical_name, (layer, table_name, required_columns) in PHASE11_REQUIRED_SOURCE_COLUMNS.items():
        schema_name = environment.gold_schema if layer == "gold" else environment.silver_schema
        table_fqn = f"{environment.catalog}.{schema_name}.{table_name}"
        tables[table_fqn] = _FakeTable(field_names=required_columns)

    validated = validate_phase11_source_contract(_FakeSpark(tables), environment)

    assert set(validated) == set(PHASE11_REQUIRED_SOURCE_COLUMNS)


def test_build_team_selection_scorecards_sql_uses_as_of_membership_dates() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_team_selection_scorecards_sql(
        environment,
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    assert "membership_start_date" in sql
    assert "membership_end_date" in sql
    assert "DATE('2025-12-31')" in sql
    assert "current_membership_flag" in sql
    assert "WITH source_teams AS" in sql
    assert "WHERE NOT TRUE" in sql
    assert "TEAM_NOT_ACTIVE" not in sql
    assert "CRITICAL_QUALITY_FAILURE" not in sql


def test_build_olympic_team_candidates_sql_filters_to_candidate_capable_rows() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_olympic_team_candidates_sql(
        environment,
        scoring_scenario="BALANCED",
        eligibility_config=_eligibility_config(),
    )

    assert "eligible_team_flag" in sql
    assert "eligibility_status = 'ELIGIBLE'" not in sql
    assert "recommendation_tier" in sql
    assert "candidate_rank" in sql


def test_publish_team_selection_scorecards_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.team_selection_scorecards"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.team_selection_scorecards"
    spark = _FakeSpark({target_fqn: _FakeTable(row_count=3)})

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection.publish_stage_records_to_gold_table",
        lambda *args, **kwargs: (3, 3),
    )

    summary = publish_team_selection_scorecards(
        spark,
        environment,
        rows=({"team_id": "team-1", "scoring_scenario": "BALANCED"},),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.output_row_count == 3


def test_publish_team_selection_scorecards_from_sql_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.team_selection_scorecards"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.team_selection_scorecards"

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection.publish_stage_to_gold_table",
        lambda *args, **kwargs: (3, 3),
    )

    summary = publish_team_selection_scorecards_from_sql(
        spark=None,
        environment=environment,
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 3
    assert summary.output_row_count == 3


def test_publish_olympic_team_candidates_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.olympic_team_candidates"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.olympic_team_candidates"
    spark = _FakeSpark({target_fqn: _FakeTable(row_count=1)})

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection.publish_stage_records_to_gold_table",
        lambda *args, **kwargs: (1, 1),
    )

    summary = publish_olympic_team_candidates(
        spark,
        environment,
        rows=(
            {
                "country_code": "USA",
                "category_code": "MENS",
                "team_id": "team-1",
                "scoring_scenario": "BALANCED",
            },
        ),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.output_row_count == 1


def test_publish_olympic_team_candidates_from_sql_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.olympic_team_candidates"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.olympic_team_candidates"

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection.publish_stage_to_gold_table",
        lambda *args, **kwargs: (1, 1),
    )

    summary = publish_olympic_team_candidates_from_sql(
        spark=None,
        environment=environment,
        scoring_scenario="BALANCED",
        eligibility_config=_eligibility_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 1
    assert summary.output_row_count == 1


def test_publish_olympic_team_candidates_supports_empty_publish(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.olympic_team_candidates"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.olympic_team_candidates"
    spark = _FakeSpark(
        {
            target_fqn: _FakeTable(row_count=0),
            stage_fqn: _FakeTable(row_count=0),
        }
    )
    published = []

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection.publish_sql_table",
        lambda _spark, table_fqn, select_sql: published.append((table_fqn, select_sql)) or 0,
    )
    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection._validate_key_constraints",
        lambda *args, **kwargs: None,
    )

    summary = publish_olympic_team_candidates(
        spark,
        environment,
        rows=(),
    )

    assert published[0][0] == stage_fqn
    assert "WHERE 1 = 0" in published[0][1]
    assert published[1][0] == target_fqn
    assert summary.input_row_count == 0
    assert summary.output_row_count == 0


def test_publish_phase11_selection_tables_returns_two_summaries(monkeypatch) -> None:
    scorecard_summary = object()
    candidate_summary = object()

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection_validation.publish_phase11_team_tables",
        lambda *args, **kwargs: type(
            "_Summary",
            (),
            {
                "team_selection_scorecards": scorecard_summary,
                "olympic_team_candidates": candidate_summary,
            },
        )(),
    )

    summary = publish_phase11_selection_tables(
        spark=None,
        environment=None,
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    assert summary.team_selection_scorecards is scorecard_summary
    assert summary.olympic_team_candidates is candidate_summary


def test_publish_phase11_team_tables_builds_scorecards_then_candidates(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection.publish_team_selection_scorecards_from_sql",
        lambda *args, **kwargs: type(
            "_Summary",
            (),
            {
                "target_table_fqn": "scorecards",
                "stage_table_fqn": "scorecards_stage",
                "input_row_count": 3,
                "output_row_count": 3,
            },
        )(),
    )
    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_selection.publish_olympic_team_candidates_from_sql",
        lambda *args, **kwargs: type(
            "_Summary",
            (),
            {
                "target_table_fqn": "candidates",
                "stage_table_fqn": "candidates_stage",
                "input_row_count": 1,
                "output_row_count": 1,
            },
        )(),
    )

    summary = publish_phase11_team_tables(
        spark=None,
        environment=environment,
        analysis_as_of_date=date(2025, 12, 31),
        scoring_scenario="BALANCED",
        scorecards_config=_scorecards_config(),
        eligibility_config=_eligibility_config(),
    )

    assert summary.team_selection_scorecards.input_row_count == 3
    assert summary.olympic_team_candidates.input_row_count == 1
