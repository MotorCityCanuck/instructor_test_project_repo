"""Tests for Raw certification Phase 4 fitness rules."""

from dataclasses import dataclass

from napa_pipeline.certification.config import load_certification_config
from napa_pipeline.certification.fitness_rules import evaluate_fitness_rules
from napa_pipeline.certification.models import (
    CertificationRuleResult,
    InventoryCertificationResult,
    SourceContract,
    SourceLoadResult,
)


@dataclass
class FakeRow:
    """Minimal Spark row stub."""

    payload: dict[str, int | float]

    def asDict(self):
        return self.payload


class FakeQueryResult:
    """Minimal Spark SQL result stub."""

    def __init__(self, payload: dict[str, int | float]):
        self.payload = payload

    def collect(self):
        return [FakeRow(self.payload)]


class FakeSparkSession:
    """Fake Spark session keyed by query tag comments."""

    def __init__(self, values_by_tag):
        self.values_by_tag = values_by_tag
        self.queries: list[str] = []

    def sql(self, query: str):
        self.queries.append(query)
        tag = query.split("*/", 1)[0].replace("/*", "").strip()
        payload = self.values_by_tag.get(tag, {"value": 0})
        return FakeQueryResult(payload)


def _inventory_result():
    config = load_certification_config("5k")
    loaded_sources = []
    expected_sources = []
    source_columns = {
        "player_master": (
            ("player_id", "string"),
            ("active_flag", "boolean"),
            ("rating", "double"),
            ("rating_confidence", "double"),
        ),
        "teams": (
            ("id", "string"),
            ("team_status", "string"),
            ("country_code", "string"),
            ("team_division", "string"),
        ),
        "team_memberships": (
            ("id", "string"),
            ("team_id", "string"),
            ("player_id", "string"),
        ),
        "match_team_players": (
            ("id", "string"),
            ("match_team_id", "string"),
            ("player_id", "string"),
        ),
        "match_teams": (
            ("id", "string"),
            ("match_id", "string"),
            ("team_id", "string"),
            ("team_number", "int"),
        ),
        "player_assessment_history": (
            ("id", "string"),
            ("player_id", "string"),
            ("assessment_date", "date"),
        ),
    }
    for source in config.sources_in_build_order:
        source_name = source["source_name"]
        columns = source_columns.get(source_name)
        if columns is None:
            continue
        loaded_sources.append(
            SourceLoadResult(
                source_name=source_name,
                file_name=source["file_name"],
                file_path=f"/Volumes/workspace/instructor_5k_raw/napa_files/{source['file_name']}",
                file_size=123,
                modification_ts=None,
                row_count=5,
                schema_hash="abc",
                schema_fields=tuple(
                    {
                        "column_name": column_name,
                        "data_type": data_type,
                        "nullable": True,
                    }
                    for column_name, data_type in columns
                ),
                temp_view_name=f"raw_cert_{source_name}",
                read_status="READY",
            )
        )
        expected_sources.append(
            SourceContract(
                source_name=source_name,
                file_name=source["file_name"],
                key_columns=tuple(source["key_columns"]),
                build_order=source["build_order"],
            )
        )
    return config, InventoryCertificationResult(
        release_name="napa_5k",
        release_path="/Volumes/workspace/instructor_5k_raw/napa_files",
        source_mode="parquet",
        path_exists=True,
        expected_sources=tuple(expected_sources),
        discovered_files=(),
        loaded_sources=tuple(loaded_sources),
        manifest=None,
        rule_results=(),
    )


def _get_rule(results: tuple[CertificationRuleResult, ...], rule_id: str) -> CertificationRuleResult:
    return next(rule for rule in results if rule.rule_id == rule_id)


def test_fitness_rules_pass_for_healthy_release() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "ACTIVE_PLAYER_RATE": {"total_players": 100, "active_players": 80},
            "VIABLE_TEAM_DEPTH": {"value": 0},
            "ZERO_MATCH_ACTIVE_PLAYERS": {
                "active_players": 80,
                "zero_match_active_players": 20,
            },
            "RATING_COVERAGE": {
                "total_players": 100,
                "rated_players": 90,
                "confidence_players": 70,
            },
            "DEVELOPMENT_HISTORY": {"value": 25},
            "RECENT_TEAM_EVIDENCE": {"value": 12},
        }
    )

    results = evaluate_fitness_rules(spark, config, inventory_result)

    assert _get_rule(results, "RAW_ACTIVE_PLAYER_RATE").status == "PASS"
    assert _get_rule(results, "RAW_VIABLE_TEAM_DEPTH_BY_COUNTRY_DIVISION").status == "PASS"
    assert _get_rule(results, "RAW_ZERO_MATCH_ACTIVE_PLAYER_RATE").status == "PASS"
    assert _get_rule(results, "RAW_PLAYER_RATING_COVERAGE").status == "PASS"
    assert _get_rule(results, "RAW_PLAYER_CONFIDENCE_COVERAGE").status == "PASS"
    assert _get_rule(results, "RAW_DEVELOPMENT_HISTORY_COVERAGE").status == "PASS"
    assert _get_rule(results, "RAW_RECENT_TEAM_EVIDENCE").status == "PASS"


def test_fitness_rules_fail_on_low_active_player_rate() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "ACTIVE_PLAYER_RATE": {"total_players": 100, "active_players": 10},
            "VIABLE_TEAM_DEPTH": {"value": 0},
            "ZERO_MATCH_ACTIVE_PLAYERS": {"active_players": 10, "zero_match_active_players": 2},
            "RATING_COVERAGE": {"total_players": 100, "rated_players": 90, "confidence_players": 90},
            "DEVELOPMENT_HISTORY": {"value": 10},
            "RECENT_TEAM_EVIDENCE": {"value": 3},
        }
    )

    results = evaluate_fitness_rules(spark, config, inventory_result)

    rule = _get_rule(results, "RAW_ACTIVE_PLAYER_RATE")
    assert rule.status == "FAIL"
    assert rule.severity == "blocker"


def test_fitness_rules_fail_on_team_depth_and_zero_match_rate() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "ACTIVE_PLAYER_RATE": {"total_players": 100, "active_players": 80},
            "VIABLE_TEAM_DEPTH": {"value": 3},
            "ZERO_MATCH_ACTIVE_PLAYERS": {
                "active_players": 80,
                "zero_match_active_players": 40,
            },
            "RATING_COVERAGE": {"total_players": 100, "rated_players": 90, "confidence_players": 90},
            "DEVELOPMENT_HISTORY": {"value": 10},
            "RECENT_TEAM_EVIDENCE": {"value": 1},
        }
    )

    results = evaluate_fitness_rules(spark, config, inventory_result)

    assert _get_rule(results, "RAW_VIABLE_TEAM_DEPTH_BY_COUNTRY_DIVISION").status == "FAIL"
    assert _get_rule(results, "RAW_ZERO_MATCH_ACTIVE_PLAYER_RATE").status == "FAIL"


def test_fitness_rules_fail_on_rating_and_confidence_coverage() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "ACTIVE_PLAYER_RATE": {"total_players": 100, "active_players": 80},
            "VIABLE_TEAM_DEPTH": {"value": 0},
            "ZERO_MATCH_ACTIVE_PLAYERS": {
                "active_players": 80,
                "zero_match_active_players": 20,
            },
            "RATING_COVERAGE": {
                "total_players": 100,
                "rated_players": 50,
                "confidence_players": 20,
            },
            "DEVELOPMENT_HISTORY": {"value": 10},
            "RECENT_TEAM_EVIDENCE": {"value": 2},
        }
    )

    results = evaluate_fitness_rules(spark, config, inventory_result)

    assert _get_rule(results, "RAW_PLAYER_RATING_COVERAGE").status == "FAIL"
    assert _get_rule(results, "RAW_PLAYER_CONFIDENCE_COVERAGE").status == "FAIL"


def test_fitness_rules_fail_on_development_and_recent_team_evidence() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "ACTIVE_PLAYER_RATE": {"total_players": 100, "active_players": 80},
            "VIABLE_TEAM_DEPTH": {"value": 0},
            "ZERO_MATCH_ACTIVE_PLAYERS": {
                "active_players": 80,
                "zero_match_active_players": 20,
            },
            "RATING_COVERAGE": {
                "total_players": 100,
                "rated_players": 90,
                "confidence_players": 70,
            },
            "DEVELOPMENT_HISTORY": {"value": 0},
            "RECENT_TEAM_EVIDENCE": {"value": 0},
        }
    )

    results = evaluate_fitness_rules(spark, config, inventory_result)

    assert _get_rule(results, "RAW_DEVELOPMENT_HISTORY_COVERAGE").status == "FAIL"
    assert _get_rule(results, "RAW_RECENT_TEAM_EVIDENCE").status == "FAIL"
