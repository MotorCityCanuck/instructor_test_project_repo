"""Tests for Raw certification structural and relationship rules."""

from dataclasses import dataclass

from napa_pipeline.certification.config import load_certification_config
from napa_pipeline.certification.models import (
    CertificationRuleResult,
    InventoryCertificationResult,
    SourceContract,
    SourceLoadResult,
)
from napa_pipeline.certification.structural_rules import evaluate_structural_rules


@dataclass
class FakeRow:
    """Minimal Spark row stub."""

    value: int

    def asDict(self):
        return {"value": self.value}


class FakeQueryResult:
    """Minimal Spark SQL result stub."""

    def __init__(self, value: int):
        self.value = value

    def collect(self):
        return [FakeRow(self.value)]


class FakeSparkSession:
    """Fake Spark session keyed by query tag comments."""

    def __init__(self, values_by_tag):
        self.values_by_tag = values_by_tag
        self.queries: list[str] = []

    def sql(self, query: str):
        self.queries.append(query)
        tag = query.split("*/", 1)[0].replace("/*", "").strip()
        return FakeQueryResult(self.values_by_tag.get(tag, 0))


def _inventory_result(schema_overrides=None):
    config = load_certification_config("5k")
    schema_overrides = schema_overrides or {}
    loaded_sources = []
    expected_sources = []
    for source in config.sources_in_build_order:
        source_name = source["source_name"]
        required_columns = config.data["schema_contract"][source_name]["required_columns"]
        schema_fields = tuple(
            {
                "column_name": column_name,
                "data_type": schema_overrides.get(source_name, {}).get(column_name, allowed_types[0]),
                "nullable": True,
            }
            for column_name, allowed_types in required_columns.items()
            if schema_overrides.get(source_name, {}).get(column_name) != "__MISSING__"
        )
        loaded_sources.append(
            SourceLoadResult(
                source_name=source_name,
                file_name=source["file_name"],
                file_path=f"/Volumes/workspace/instructor_5k_raw/napa_files/{source['file_name']}",
                file_size=123,
                modification_ts=None,
                row_count=5,
                schema_hash="abc",
                schema_fields=schema_fields,
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


def test_structural_rules_pass_for_clean_release() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession({})

    results = evaluate_structural_rules(spark, config, inventory_result)

    assert _get_rule(results, "RAW_SCHEMA_REQUIRED_COLUMNS_TEAMS").status == "PASS"
    assert _get_rule(results, "RAW_SCHEMA_TYPE_COMPATIBILITY_MATCH_GAMES").status == "PASS"
    assert _get_rule(results, "RAW_PRIMARY_KEY_UNIQUENESS_MATCHES").status == "PASS"
    assert _get_rule(results, "RAW_FOREIGN_KEY_TEAM_MEMBERSHIPS_TEAM").status == "PASS"
    assert _get_rule(results, "RAW_MATCH_WINNER_INTEGRITY").status == "PASS"
    assert _get_rule(results, "RAW_PERSISTENT_TEAM_IDENTITY_INVARIANTS").status == "PASS"


def test_structural_rules_fail_on_missing_required_column() -> None:
    config, inventory_result = _inventory_result(
        schema_overrides={"teams": {"team_division": "__MISSING__"}}
    )
    spark = FakeSparkSession({})

    results = evaluate_structural_rules(spark, config, inventory_result)

    rule = _get_rule(results, "RAW_SCHEMA_REQUIRED_COLUMNS_TEAMS")
    assert rule.status == "FAIL"
    assert "team_division" in rule.sample_records


def test_structural_rules_fail_on_incompatible_type() -> None:
    config, inventory_result = _inventory_result(
        schema_overrides={"match_games": {"game_number": "string"}}
    )
    spark = FakeSparkSession({})

    results = evaluate_structural_rules(spark, config, inventory_result)

    rule = _get_rule(results, "RAW_SCHEMA_TYPE_COMPATIBILITY_MATCH_GAMES")
    assert rule.status == "FAIL"
    assert "game_number:string" in rule.sample_records


def test_structural_rules_fail_on_duplicate_primary_keys() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession({"PK_DUPLICATES:matches": 2})

    results = evaluate_structural_rules(spark, config, inventory_result)

    rule = _get_rule(results, "RAW_PRIMARY_KEY_UNIQUENESS_MATCHES")
    assert rule.status == "FAIL"
    assert rule.affected_count == 2


def test_structural_rules_fail_on_orphan_foreign_keys() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession({"FK_ORPHANS:RAW_FOREIGN_KEY_MATCH_TEAMS_MATCH": 3})

    results = evaluate_structural_rules(spark, config, inventory_result)

    rule = _get_rule(results, "RAW_FOREIGN_KEY_MATCH_TEAMS_MATCH")
    assert rule.status == "FAIL"
    assert rule.affected_count == 3


def test_structural_rules_fail_on_match_winner_integrity() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession({"MATCH_WINNER_INTEGRITY": 4})

    results = evaluate_structural_rules(spark, config, inventory_result)

    rule = _get_rule(results, "RAW_MATCH_WINNER_INTEGRITY")
    assert rule.status == "FAIL"
    assert rule.affected_count == 4


def test_structural_rules_fail_on_game_sequence_and_score_integrity() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession({"MATCH_GAME_SEQUENCE": 1, "MATCH_GAME_SCORE": 5})

    results = evaluate_structural_rules(spark, config, inventory_result)

    assert _get_rule(results, "RAW_MATCH_GAME_SEQUENCE_INTEGRITY").status == "FAIL"
    assert _get_rule(results, "RAW_MATCH_GAME_SCORE_INTEGRITY").affected_count == 5


def test_structural_rules_fail_on_team_identity_invariant() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession({"TEAM_IDENTITY_INVARIANTS": 2})

    results = evaluate_structural_rules(spark, config, inventory_result)

    rule = _get_rule(results, "RAW_PERSISTENT_TEAM_IDENTITY_INVARIANTS")
    assert rule.status == "FAIL"
    assert rule.affected_count == 2
