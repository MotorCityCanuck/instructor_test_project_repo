"""Tests for Silver-to-Gold Phase 5 analytical rating builders."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.ratings import (
    PLAYER_CURRENT_RATINGS_SCHEMA,
    PLAYER_RATING_EVENTS_SCHEMA,
    PLAYER_RATING_HISTORY_SCHEMA,
    build_player_current_ratings,
    build_player_rating_events,
    build_player_rating_history,
    expected_win_probability,
    experience_multiplier,
    publish_player_rating_events,
)
from napa_pipeline.silver_to_gold.ratings_validation import (
    PHASE5_REQUIRED_SOURCE_COLUMNS,
    publish_phase5_rating_tables,
    validate_phase5_source_contract,
)


def _ratings_config():
    return load_silver_to_gold_config("napa_5k").data["ratings"]


def _competition_player_matches_rows():
    return [
        {
            "match_id": "match-2",
            "match_date": "2026-06-15",
            "batch_id": "batch-1",
            "batch_sequence": 1,
            "batch_date": "2026-06-30",
            "team_number": 2,
            "player_id": "player-3",
            "partner_player_id": "player-4",
            "pre_match_player_rating": 4.2,
            "won_flag": True,
            "lost_flag": False,
            "player_position": "LEFT",
        },
        {
            "match_id": "match-1",
            "match_date": "2026-06-15",
            "batch_id": "batch-1",
            "batch_sequence": 1,
            "batch_date": "2026-06-30",
            "team_number": 1,
            "player_id": "player-1",
            "partner_player_id": "player-2",
            "pre_match_player_rating": 4.4,
            "won_flag": True,
            "lost_flag": False,
            "player_position": "LEFT",
        },
        {
            "match_id": "match-1",
            "match_date": "2026-06-15",
            "batch_id": "batch-1",
            "batch_sequence": 1,
            "batch_date": "2026-06-30",
            "team_number": 1,
            "player_id": "player-2",
            "partner_player_id": "player-1",
            "pre_match_player_rating": 4.3,
            "won_flag": True,
            "lost_flag": False,
            "player_position": "RIGHT",
        },
        {
            "match_id": "match-1",
            "match_date": "2026-06-15",
            "batch_id": "batch-1",
            "batch_sequence": 1,
            "batch_date": "2026-06-30",
            "team_number": 2,
            "player_id": "player-3",
            "partner_player_id": "player-4",
            "pre_match_player_rating": 4.2,
            "won_flag": False,
            "lost_flag": True,
            "player_position": "LEFT",
        },
        {
            "match_id": "match-1",
            "match_date": "2026-06-15",
            "batch_id": "batch-1",
            "batch_sequence": 1,
            "batch_date": "2026-06-30",
            "team_number": 2,
            "player_id": "player-4",
            "partner_player_id": "player-3",
            "pre_match_player_rating": 4.1,
            "won_flag": False,
            "lost_flag": True,
            "player_position": "RIGHT",
        },
        {
            "match_id": "match-2",
            "match_date": "2026-06-15",
            "batch_id": "batch-1",
            "batch_sequence": 1,
            "batch_date": "2026-06-30",
            "team_number": 2,
            "player_id": "player-4",
            "partner_player_id": "player-3",
            "pre_match_player_rating": 4.1,
            "won_flag": True,
            "lost_flag": False,
            "player_position": "RIGHT",
        },
        {
            "match_id": "match-2",
            "match_date": "2026-06-15",
            "batch_id": "batch-1",
            "batch_sequence": 1,
            "batch_date": "2026-06-30",
            "team_number": 1,
            "player_id": "player-1",
            "partner_player_id": "player-2",
            "pre_match_player_rating": 4.4,
            "won_flag": False,
            "lost_flag": True,
            "player_position": "LEFT",
        },
        {
            "match_id": "match-2",
            "match_date": "2026-06-15",
            "batch_id": "batch-1",
            "batch_sequence": 1,
            "batch_date": "2026-06-30",
            "team_number": 1,
            "player_id": "player-2",
            "partner_player_id": "player-1",
            "pre_match_player_rating": 4.3,
            "won_flag": False,
            "lost_flag": True,
            "player_position": "RIGHT",
        },
    ]


def _players_rows():
    return [
        {
            "player_id": "player-1",
            "display_name": "Player One",
            "country_code": "USA",
            "active_flag": True,
            "rating": 4.4,
            "rating_confidence": 0.90,
        },
        {
            "player_id": "player-2",
            "display_name": "Player Two",
            "country_code": "USA",
            "active_flag": True,
            "rating": 4.3,
            "rating_confidence": 0.85,
        },
        {
            "player_id": "player-3",
            "display_name": "Player Three",
            "country_code": "CAN",
            "active_flag": True,
            "rating": 4.2,
            "rating_confidence": 0.80,
        },
        {
            "player_id": "player-4",
            "display_name": "Player Four",
            "country_code": "CAN",
            "active_flag": True,
            "rating": 4.1,
            "rating_confidence": 0.75,
        },
        {
            "player_id": "player-5",
            "display_name": "Player Five",
            "country_code": "USA",
            "active_flag": False,
            "rating": 4.0,
            "rating_confidence": 0.60,
        },
    ]


def test_expected_win_probability_equal_ratings_returns_half() -> None:
    assert expected_win_probability(1500.0, 1500.0) == 0.5


def test_experience_multiplier_uses_expected_tiers() -> None:
    assert experience_multiplier(0) == 1.50
    assert experience_multiplier(10) == 1.25
    assert experience_multiplier(25) == 1.00
    assert experience_multiplier(75) == 0.80


def test_build_player_rating_events_is_deterministic_and_zero_sum() -> None:
    first = build_player_rating_events(
        _competition_player_matches_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )
    second = build_player_rating_events(
        list(reversed(_competition_player_matches_rows())),
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    assert first.rows == second.rows
    assert len(first.rows) == 8

    match_one_rows = [row for row in first.rows if row["match_id"] == "match-1"]
    match_two_rows = [row for row in first.rows if row["match_id"] == "match-2"]
    assert round(sum(row["rating_delta"] for row in match_one_rows), 10) == 0.0
    assert round(sum(row["rating_delta"] for row in match_two_rows), 10) == 0.0

    first_match_player_one = next(
        row for row in first.rows if row["match_id"] == "match-1" and row["player_id"] == "player-1"
    )
    second_match_player_one = next(
        row for row in first.rows if row["match_id"] == "match-2" and row["player_id"] == "player-1"
    )
    second_match_player_three = next(
        row for row in first.rows if row["match_id"] == "match-2" and row["player_id"] == "player-3"
    )

    assert first_match_player_one["prior_match_count"] == 0
    assert first_match_player_one["post_match_count"] == 1
    assert first_match_player_one["pre_match_rating"] == 1500.0
    assert first_match_player_one["post_match_rating"] > first_match_player_one["pre_match_rating"]
    assert second_match_player_one["prior_match_count"] == 1
    assert second_match_player_one["expected_win_probability"] > 0.5
    assert second_match_player_one["rating_delta"] < 0.0
    assert second_match_player_three["expected_win_probability"] < 0.5
    assert second_match_player_three["rating_delta"] > 0.0


def test_build_player_rating_history_collapses_same_day_matches() -> None:
    events = build_player_rating_events(
        _competition_player_matches_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    history = build_player_rating_history(
        events.rows,
        _players_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    assert len(history.rows) == 4
    player_one_history = next(row for row in history.rows if row["player_id"] == "player-1")
    assert player_one_history["rating_effective_date"] == date(2026, 6, 15)
    assert player_one_history["rated_match_count"] == 2
    assert player_one_history["wins_to_date"] == 1
    assert player_one_history["losses_to_date"] == 1
    assert player_one_history["is_current_flag"] is True


def test_build_player_current_ratings_uses_latest_history_and_includes_unrated_players() -> None:
    events = build_player_rating_events(
        _competition_player_matches_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )
    history = build_player_rating_history(
        events.rows,
        _players_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    current = build_player_current_ratings(
        history.rows,
        _players_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    assert len(current.rows) == 5
    player_one = next(row for row in current.rows if row["player_id"] == "player-1")
    player_five = next(row for row in current.rows if row["player_id"] == "player-5")
    history_player_one = next(row for row in history.rows if row["player_id"] == "player-1")

    assert player_one["analytical_rating_value"] == history_player_one["analytical_rating_value"]
    assert player_one["current_rating_effective_date"] == date(2026, 6, 15)
    assert player_five["analytical_rating_value"] == 1500.0
    assert player_five["rated_match_count"] == 0
    assert player_five["analytical_rating_rank_overall"] >= 1


class _FakeField:
    def __init__(self, name: str):
        self.name = name


class _FakeSchema:
    def __init__(self, field_names):
        self.fields = [_FakeField(name) for name in field_names]


class _FakeTable:
    def __init__(self, *, field_names=None, row_count: int = 0, rows=None):
        self.schema = _FakeSchema(field_names or [])
        self._row_count = row_count
        self._rows = list(rows or [])

    def count(self) -> int:
        return self._row_count

    def toLocalIterator(self):
        return iter(self._rows)


class _FakeSpark:
    def __init__(self, tables):
        self._tables = tables

    def table(self, table_name: str):
        return self._tables[table_name]


def test_validate_phase5_source_contract_checks_gold_and_silver_tables() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    tables = {}
    for logical_name, (layer, table_name, required_columns) in PHASE5_REQUIRED_SOURCE_COLUMNS.items():
        schema_name = environment.gold_schema if layer == "gold" else environment.silver_schema
        table_fqn = f"{environment.catalog}.{schema_name}.{table_name}"
        tables[table_fqn] = _FakeTable(field_names=required_columns)

    validated = validate_phase5_source_contract(_FakeSpark(tables), environment)

    assert set(validated) == set(PHASE5_REQUIRED_SOURCE_COLUMNS)


def test_publish_player_rating_events_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    source_fqn = f"{environment.catalog}.{environment.gold_schema}.competition_player_matches"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.player_rating_events"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.player_rating_events"
    spark = _FakeSpark(
        {
            source_fqn: _FakeTable(row_count=8, rows=_competition_player_matches_rows()),
            target_fqn: _FakeTable(row_count=8),
        }
    )
    published = {}

    def _fake_publish_stage_records_to_gold_table(
        _spark,
        *,
        stage_table_fqn: str,
        target_table_fqn: str,
        records,
        schema=None,
        validation_fn=None,
        count_fn=None,
    ):
        published["stage_table_fqn"] = stage_table_fqn
        published["target_table_fqn"] = target_table_fqn
        published["row_count"] = len(records)
        published["schema"] = schema
        return len(records), len(records)

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.ratings.publish_stage_records_to_gold_table",
        _fake_publish_stage_records_to_gold_table,
    )

    summary = publish_player_rating_events(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 8
    assert summary.output_row_count == 8
    assert published["row_count"] == 8
    assert published["schema"] is PLAYER_RATING_EVENTS_SCHEMA


def test_publish_player_rating_history_passes_explicit_schema(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    events_fqn = f"{environment.catalog}.{environment.gold_schema}.player_rating_events"
    players_fqn = f"{environment.catalog}.{environment.silver_schema}.players"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.player_rating_history"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.player_rating_history"
    spark = _FakeSpark(
        {
            events_fqn: _FakeTable(row_count=8, rows=build_player_rating_events(
                _competition_player_matches_rows(),
                analysis_as_of_date=date(2026, 6, 30),
                ratings_config=_ratings_config(),
            ).rows),
            players_fqn: _FakeTable(row_count=5, rows=_players_rows()),
            target_fqn: _FakeTable(row_count=4),
        }
    )
    published = {}

    def _fake_publish_stage_records_to_gold_table(
        _spark,
        *,
        stage_table_fqn: str,
        target_table_fqn: str,
        records,
        schema=None,
        validation_fn=None,
        count_fn=None,
    ):
        published["stage_table_fqn"] = stage_table_fqn
        published["target_table_fqn"] = target_table_fqn
        published["row_count"] = len(records)
        published["schema"] = schema
        return len(records), len(records)

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.ratings.publish_stage_records_to_gold_table",
        _fake_publish_stage_records_to_gold_table,
    )

    from napa_pipeline.silver_to_gold.ratings import publish_player_rating_history

    summary = publish_player_rating_history(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 8
    assert summary.output_row_count == 4
    assert published["schema"] is PLAYER_RATING_HISTORY_SCHEMA


def test_publish_player_current_ratings_passes_explicit_schema(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    history_rows = build_player_rating_history(
        build_player_rating_events(
            _competition_player_matches_rows(),
            analysis_as_of_date=date(2026, 6, 30),
            ratings_config=_ratings_config(),
        ).rows,
        _players_rows(),
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    ).rows
    history_fqn = f"{environment.catalog}.{environment.gold_schema}.player_rating_history"
    players_fqn = f"{environment.catalog}.{environment.silver_schema}.players"
    target_fqn = f"{environment.catalog}.{environment.gold_schema}.player_current_ratings"
    stage_fqn = f"{environment.catalog}.{environment.gold_stage_schema}.player_current_ratings"
    spark = _FakeSpark(
        {
            history_fqn: _FakeTable(row_count=4, rows=history_rows),
            players_fqn: _FakeTable(row_count=5, rows=_players_rows()),
            target_fqn: _FakeTable(row_count=5),
        }
    )
    published = {}

    def _fake_publish_stage_records_to_gold_table(
        _spark,
        *,
        stage_table_fqn: str,
        target_table_fqn: str,
        records,
        schema=None,
        validation_fn=None,
        count_fn=None,
    ):
        published["stage_table_fqn"] = stage_table_fqn
        published["target_table_fqn"] = target_table_fqn
        published["row_count"] = len(records)
        published["schema"] = schema
        return len(records), len(records)

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.ratings.publish_stage_records_to_gold_table",
        _fake_publish_stage_records_to_gold_table,
    )

    from napa_pipeline.silver_to_gold.ratings import publish_player_current_ratings

    summary = publish_player_current_ratings(
        spark,
        environment,
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    assert summary.stage_table_fqn == stage_fqn
    assert summary.target_table_fqn == target_fqn
    assert summary.input_row_count == 5
    assert summary.output_row_count == 5
    assert published["schema"] is PLAYER_CURRENT_RATINGS_SCHEMA


def test_publish_phase5_rating_tables_returns_three_summaries(monkeypatch) -> None:
    events_summary = object()
    history_summary = object()
    current_summary = object()

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.ratings_validation.publish_player_rating_events",
        lambda *args, **kwargs: events_summary,
    )
    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.ratings_validation.publish_player_rating_history",
        lambda *args, **kwargs: history_summary,
    )
    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.ratings_validation.publish_player_current_ratings",
        lambda *args, **kwargs: current_summary,
    )

    summary = publish_phase5_rating_tables(
        spark=None,
        environment=None,
        analysis_as_of_date=date(2026, 6, 30),
        ratings_config=_ratings_config(),
    )

    assert summary.player_rating_events is events_summary
    assert summary.player_rating_history is history_summary
    assert summary.player_current_ratings is current_summary
