"""Tests for Silver-to-Gold persistent-team resolution."""

from datetime import date

from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import resolve_release_environment
from napa_pipeline.silver_to_gold.team_resolution import (
    ACTIVE_MEMBERSHIP_PAIR,
    AMBIGUOUS,
    DIRECT_VALID_TEAM_ID,
    RESOLVED,
    UNIQUE_HISTORICAL_PAIR,
    UNRESOLVED,
    build_resolved_match_teams_sql,
    build_resolved_match_teams,
    publish_resolved_match_teams,
)


def _teams_rows():
    return [
        {
            "team_id": "team-direct",
            "team_name": "Direct Team",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2026-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-active-pair",
            "team_name": "Active Pair Team",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2026-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-historical",
            "team_name": "Historical Team",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2024-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-ambiguous-a",
            "team_name": "Ambiguous A",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2025-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-ambiguous-b",
            "team_name": "Ambiguous B",
            "team_status": "ACTIVE",
            "active_flag": True,
            "formation_date": "2025-01-01",
            "dissolution_date": None,
        },
        {
            "team_id": "team-dissolved",
            "team_name": "Dissolved Team",
            "team_status": "DISSOLVED",
            "active_flag": False,
            "formation_date": "2024-01-01",
            "dissolution_date": "2026-05-01",
        },
    ]


def test_build_resolved_match_teams_direct_resolution_uses_valid_team_id() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-1",
                "match_id": "match-1",
                "team_id": "team-direct",
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-1", "player_id": "player-2"},
            {"match_team_id": "mt-1", "player_id": "player-1"},
        ],
        team_memberships_rows=[],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["canonical_player_pair_key"] == "player-1:player-2"
    assert row["resolved_team_id"] == "team-direct"
    assert row["team_resolution_method"] == DIRECT_VALID_TEAM_ID
    assert row["team_resolution_status"] == RESOLVED
    assert row["candidate_attribution_allowed_flag"] is True
    assert result.direct_resolution_count == 1


def test_build_resolved_match_teams_uses_active_membership_pair_when_direct_team_missing() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-2",
                "match_id": "match-2",
                "team_id": None,
                "team_number": 2,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-2", "player_id": "player-3"},
            {"match_team_id": "mt-2", "player_id": "player-4"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-active-pair",
                "player_id": "player-3",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-active-pair",
                "player_id": "player-4",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["resolved_team_id"] == "team-active-pair"
    assert row["team_resolution_method"] == ACTIVE_MEMBERSHIP_PAIR
    assert row["team_resolution_status"] == RESOLVED
    assert row["team_resolution_confidence"] == 0.9
    assert result.active_pair_resolution_count == 1


def test_build_resolved_match_teams_uses_unique_historical_pair_when_no_active_pair_exists() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-3",
                "match_id": "match-3",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-3", "player_id": "player-5"},
            {"match_team_id": "mt-3", "player_id": "player-6"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-historical",
                "player_id": "player-5",
                "membership_start_date": "2025-01-01",
                "membership_end_date": "2025-05-31",
            },
            {
                "team_id": "team-historical",
                "player_id": "player-6",
                "membership_start_date": "2025-01-15",
                "membership_end_date": "2025-06-15",
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["resolved_team_id"] == "team-historical"
    assert row["team_resolution_method"] == UNIQUE_HISTORICAL_PAIR
    assert row["team_resolution_status"] == RESOLVED
    assert row["team_resolution_confidence"] == 0.6
    assert result.historical_pair_resolution_count == 1


def test_build_resolved_match_teams_marks_active_pair_overlaps_as_ambiguous() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-4",
                "match_id": "match-4",
                "team_id": None,
                "team_number": 2,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-4", "player_id": "player-7"},
            {"match_team_id": "mt-4", "player_id": "player-8"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-ambiguous-a",
                "player_id": "player-7",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-ambiguous-a",
                "player_id": "player-8",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-ambiguous-b",
                "player_id": "player-7",
                "membership_start_date": "2026-02-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-ambiguous-b",
                "player_id": "player-8",
                "membership_start_date": "2026-02-01",
                "membership_end_date": None,
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["resolved_team_id"] is None
    assert row["team_resolution_method"] == AMBIGUOUS
    assert row["team_resolution_status"] == AMBIGUOUS
    assert row["candidate_attribution_allowed_flag"] is False
    assert result.ambiguous_count == 1


def test_build_resolved_match_teams_marks_historical_pair_multiple_teams_as_ambiguous() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-5",
                "match_id": "match-5",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-5", "player_id": "player-9"},
            {"match_team_id": "mt-5", "player_id": "player-10"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-ambiguous-a",
                "player_id": "player-9",
                "membership_start_date": "2025-01-01",
                "membership_end_date": "2025-03-31",
            },
            {
                "team_id": "team-ambiguous-a",
                "player_id": "player-10",
                "membership_start_date": "2025-01-01",
                "membership_end_date": "2025-03-31",
            },
            {
                "team_id": "team-ambiguous-b",
                "player_id": "player-9",
                "membership_start_date": "2025-04-01",
                "membership_end_date": "2025-06-30",
            },
            {
                "team_id": "team-ambiguous-b",
                "player_id": "player-10",
                "membership_start_date": "2025-04-01",
                "membership_end_date": "2025-06-30",
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["team_resolution_status"] == AMBIGUOUS
    assert row["resolved_team_id"] is None


def test_build_resolved_match_teams_marks_missing_team_history_as_unresolved() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-6",
                "match_id": "match-6",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-6", "player_id": "player-11"},
            {"match_team_id": "mt-6", "player_id": "player-12"},
        ],
        team_memberships_rows=[],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["team_resolution_method"] == UNRESOLVED
    assert row["team_resolution_status"] == UNRESOLVED
    assert result.unresolved_count == 1


def test_build_resolved_match_teams_resolves_dissolved_team_but_blocks_candidate_attribution() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-7",
                "match_id": "match-7",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-04-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-7", "player_id": "player-13"},
            {"match_team_id": "mt-7", "player_id": "player-14"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-dissolved",
                "player_id": "player-13",
                "membership_start_date": "2026-01-01",
                "membership_end_date": "2026-05-01",
            },
            {
                "team_id": "team-dissolved",
                "player_id": "player-14",
                "membership_start_date": "2026-01-01",
                "membership_end_date": "2026-05-01",
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["resolved_team_id"] == "team-dissolved"
    assert row["team_resolution_method"] == ACTIVE_MEMBERSHIP_PAIR
    assert row["candidate_attribution_allowed_flag"] is False


def test_build_resolved_match_teams_treats_membership_date_boundaries_as_inclusive() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-8",
                "match_id": "match-8",
                "team_id": None,
                "team_number": 2,
                "match_date": date(2026, 6, 15),
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-8", "player_id": "player-15"},
            {"match_team_id": "mt-8", "player_id": "player-16"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-active-pair",
                "player_id": "player-15",
                "membership_start_date": "2026-06-15",
                "membership_end_date": "2026-07-01",
            },
            {
                "team_id": "team-active-pair",
                "player_id": "player-16",
                "membership_start_date": "2026-01-01",
                "membership_end_date": "2026-06-15",
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["team_resolution_method"] == ACTIVE_MEMBERSHIP_PAIR
    assert row["resolved_team_id"] == "team-active-pair"


def test_build_resolved_match_teams_normalizes_reversed_player_order_and_duplicate_memberships() -> None:
    result = build_resolved_match_teams(
        match_teams_rows=[
            {
                "match_team_id": "mt-9",
                "match_id": "match-9",
                "team_id": None,
                "team_number": 1,
                "match_date": "2026-06-15",
            }
        ],
        match_team_players_rows=[
            {"match_team_id": "mt-9", "player_id": "player-18"},
            {"match_team_id": "mt-9", "player_id": "player-17"},
        ],
        team_memberships_rows=[
            {
                "team_id": "team-active-pair",
                "player_id": "player-17",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-active-pair",
                "player_id": "player-17",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
            {
                "team_id": "team-active-pair",
                "player_id": "player-18",
                "membership_start_date": "2026-01-01",
                "membership_end_date": None,
            },
        ],
        teams_rows=_teams_rows(),
    )

    row = result.rows[0]
    assert row["canonical_player_pair_key"] == "player-17:player-18"
    assert row["team_resolution_method"] == ACTIVE_MEMBERSHIP_PAIR
    assert row["resolved_team_id"] == "team-active-pair"


def test_build_resolved_match_teams_sql_references_required_sources() -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)

    sql = build_resolved_match_teams_sql(environment)

    assert f"{environment.catalog}.{environment.silver_schema}.match_teams" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.match_team_players" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.team_memberships" in sql
    assert f"{environment.catalog}.{environment.silver_schema}.teams" in sql
    assert "candidate_attribution_allowed_flag" in sql
    assert "team_resolution_confidence" in sql


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


class _FakeCountTable:
    def __init__(self, row_count: int):
        self._row_count = row_count

    def count(self):
        return self._row_count


class _FakeSpark:
    def __init__(self, input_row_count: int, summary_row: dict[str, object]):
        self._input_row_count = input_row_count
        self._summary_row = summary_row
        self.executed_sql: list[str] = []
        self.requested_tables: list[str] = []

    def table(self, table_name: str):
        self.requested_tables.append(table_name)
        return _FakeCountTable(self._input_row_count)

    def sql(self, query: str):
        self.executed_sql.append(query)
        return _FakeCollectResult([self._summary_row])


def test_publish_resolved_match_teams_returns_summary(monkeypatch) -> None:
    config = load_silver_to_gold_config("napa_5k")
    environment = resolve_release_environment(config)
    spark = _FakeSpark(
        input_row_count=156148,
        summary_row={
            "output_row_count": 156148,
            "direct_resolution_count": 80000,
            "active_pair_resolution_count": 50000,
            "historical_pair_resolution_count": 10000,
            "ambiguous_count": 5000,
            "unresolved_count": 1148,
            "persistent_team_resolution_pct": 89.0,
        },
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
        return 156148, 156148

    monkeypatch.setattr(
        "napa_pipeline.silver_to_gold.team_resolution.publish_stage_to_gold_table",
        _fake_publish_stage_to_gold_table,
    )

    summary = publish_resolved_match_teams(spark, environment)

    assert summary.input_row_count == 156148
    assert summary.output_row_count == 156148
    assert summary.direct_resolution_count == 80000
    assert summary.active_pair_resolution_count == 50000
    assert summary.historical_pair_resolution_count == 10000
    assert summary.ambiguous_count == 5000
    assert summary.unresolved_count == 1148
    assert summary.persistent_team_resolution_pct == 89.0
    assert published["target_table_fqn"] == f"{environment.catalog}.{environment.gold_schema}.resolved_match_teams"
    assert published["stage_table_fqn"] == f"{environment.catalog}.{environment.gold_stage_schema}.resolved_match_teams"
    assert "DIRECT_VALID_TEAM_ID" in published["stage_sql"]
