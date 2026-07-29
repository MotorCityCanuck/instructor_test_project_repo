"""Tests for Raw certification assignment pathway probes."""

from dataclasses import dataclass

from napa_pipeline.certification.assignment_probes import evaluate_assignment_probes
from napa_pipeline.certification.config import load_certification_config
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
    source_columns = {
        "player_master": (
            ("player_id", "string"),
            ("country_code", "string"),
            ("rating", "double"),
            ("rating_confidence", "double"),
            ("preferred_division", "string"),
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
    loaded_sources = []
    expected_sources = []
    for source in config.sources_in_build_order:
        columns = source_columns.get(source["source_name"])
        if columns is None:
            continue
        loaded_sources.append(
            SourceLoadResult(
                source_name=source["source_name"],
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
                temp_view_name=f"raw_cert_{source['source_name']}",
                read_status="READY",
            )
        )
        expected_sources.append(
            SourceContract(
                source_name=source["source_name"],
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


def test_assignment_probes_pass_for_viable_release() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "PROBE_NATIONAL_RANKING": {"value": 0},
            "PROBE_PLAYER_SELECTION": {"value": 0},
            "PROBE_TEAM_SELECTION": {"value": 0},
            "PROBE_PARTNERSHIP": {"value": 25},
            "PROBE_DEVELOPMENT": {"value": 10},
            "PROBE_TOURNAMENT": {"value": 0},
        }
    )
    structural_results = (
        CertificationRuleResult("R1", "r1", "p", "c", "FAIL", "warning", "m", affected_count=2),
        CertificationRuleResult("R2", "r2", "p", "c", "PASS", "info", "m", affected_count=0),
    )

    results = evaluate_assignment_probes(spark, config, inventory_result, structural_results)

    assert _get_rule(results, "RAW_PROBE_NATIONAL_RANKING").status == "PASS"
    assert _get_rule(results, "RAW_PROBE_OLYMPIC_PLAYER_SELECTION").status == "PASS"
    assert _get_rule(results, "RAW_PROBE_OLYMPIC_TEAM_SELECTION").status == "PASS"
    assert _get_rule(results, "RAW_PROBE_PARTNERSHIP_ANALYSIS").status == "PASS"
    assert _get_rule(results, "RAW_PROBE_FUTURE_DEVELOPMENT").status == "PASS"
    assert _get_rule(results, "RAW_PROBE_TOURNAMENT_CANDIDATES").status == "PASS"
    assert _get_rule(results, "RAW_PROBE_DATA_QUALITY_LEARNING").status == "PASS"


def test_assignment_probes_fail_on_pathway_depth_gaps() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "PROBE_NATIONAL_RANKING": {"value": 1},
            "PROBE_PLAYER_SELECTION": {"value": 2},
            "PROBE_TEAM_SELECTION": {"value": 3},
            "PROBE_PARTNERSHIP": {"value": 1},
            "PROBE_DEVELOPMENT": {"value": 0},
            "PROBE_TOURNAMENT": {"value": 2},
        }
    )
    structural_results = (
        CertificationRuleResult("R1", "r1", "p", "c", "FAIL", "warning", "m", affected_count=2),
    )

    results = evaluate_assignment_probes(spark, config, inventory_result, structural_results)

    assert _get_rule(results, "RAW_PROBE_NATIONAL_RANKING").status == "FAIL"
    assert _get_rule(results, "RAW_PROBE_OLYMPIC_PLAYER_SELECTION").status == "FAIL"
    assert _get_rule(results, "RAW_PROBE_OLYMPIC_TEAM_SELECTION").status == "FAIL"
    assert _get_rule(results, "RAW_PROBE_PARTNERSHIP_ANALYSIS").status == "FAIL"
    assert _get_rule(results, "RAW_PROBE_FUTURE_DEVELOPMENT").status == "FAIL"
    assert _get_rule(results, "RAW_PROBE_TOURNAMENT_CANDIDATES").status == "FAIL"


def test_assignment_probes_fail_when_quality_signal_is_too_sterile() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "PROBE_NATIONAL_RANKING": {"value": 0},
            "PROBE_PLAYER_SELECTION": {"value": 0},
            "PROBE_TEAM_SELECTION": {"value": 0},
            "PROBE_PARTNERSHIP": {"value": 25},
            "PROBE_DEVELOPMENT": {"value": 10},
            "PROBE_TOURNAMENT": {"value": 0},
        }
    )
    structural_results = (
        CertificationRuleResult("R1", "r1", "p", "c", "PASS", "info", "m", affected_count=0),
    )

    results = evaluate_assignment_probes(spark, config, inventory_result, structural_results)

    rule = _get_rule(results, "RAW_PROBE_DATA_QUALITY_LEARNING")
    assert rule.status == "FAIL"
    assert rule.severity == "warning"


def test_assignment_probes_fail_when_quality_signal_is_too_large() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "PROBE_NATIONAL_RANKING": {"value": 0},
            "PROBE_PLAYER_SELECTION": {"value": 0},
            "PROBE_TEAM_SELECTION": {"value": 0},
            "PROBE_PARTNERSHIP": {"value": 25},
            "PROBE_DEVELOPMENT": {"value": 10},
            "PROBE_TOURNAMENT": {"value": 0},
        }
    )
    structural_results = tuple(
        CertificationRuleResult(f"R{i}", "r", "p", "c", "FAIL", "error", "m", affected_count=5)
        for i in range(1, 5)
    )

    results = evaluate_assignment_probes(spark, config, inventory_result, structural_results)

    rule = _get_rule(results, "RAW_PROBE_DATA_QUALITY_LEARNING")
    assert rule.status == "FAIL"
    assert rule.severity == "error"


def test_assignment_probes_accept_confidence_score_alias() -> None:
    config, inventory_result = _inventory_result()
    player_source = next(
        source for source in inventory_result.loaded_sources if source.source_name == "player_master"
    )
    aliased_player_source = SourceLoadResult(
        source_name=player_source.source_name,
        file_name=player_source.file_name,
        file_path=player_source.file_path,
        file_size=player_source.file_size,
        modification_ts=player_source.modification_ts,
        row_count=player_source.row_count,
        schema_hash=player_source.schema_hash,
        schema_fields=tuple(
            {
                "column_name": "confidence_score" if field["column_name"] == "rating_confidence" else field["column_name"],
                "data_type": field["data_type"],
                "nullable": field["nullable"],
            }
            for field in player_source.schema_fields
        ),
        temp_view_name=player_source.temp_view_name,
        read_status=player_source.read_status,
    )
    inventory_result = InventoryCertificationResult(
        release_name=inventory_result.release_name,
        release_path=inventory_result.release_path,
        source_mode=inventory_result.source_mode,
        path_exists=inventory_result.path_exists,
        expected_sources=inventory_result.expected_sources,
        discovered_files=inventory_result.discovered_files,
        loaded_sources=tuple(
            aliased_player_source if source.source_name == "player_master" else source
            for source in inventory_result.loaded_sources
        ),
        manifest=inventory_result.manifest,
        rule_results=inventory_result.rule_results,
    )
    spark = FakeSparkSession(
        {
            "PROBE_NATIONAL_RANKING": {"value": 0},
            "PROBE_PLAYER_SELECTION": {"value": 0},
            "PROBE_TEAM_SELECTION": {"value": 0},
            "PROBE_PARTNERSHIP": {"value": 25},
            "PROBE_DEVELOPMENT": {"value": 10},
            "PROBE_TOURNAMENT": {"value": 0},
        }
    )

    results = evaluate_assignment_probes(spark, config, inventory_result, ())

    assert _get_rule(results, "RAW_PROBE_OLYMPIC_PLAYER_SELECTION").status == "PASS"
    assert "confidence_score" in next(
        query for query in spark.queries if "PROBE_PLAYER_SELECTION" in query
    )
