"""Tests for Raw certification source reconciliation and regression rules."""

from dataclasses import dataclass
import json

from napa_pipeline.certification.config import load_certification_config
from napa_pipeline.certification.models import InventoryCertificationResult, SourceContract, SourceLoadResult
from napa_pipeline.certification.reconciliation import (
    build_release_metrics,
    evaluate_reconciliation_and_regression_rules,
    load_certification_snapshot,
)


@dataclass
class FakeRow:
    """Minimal Spark row stub."""

    payload: dict[str, int | float | str]

    def asDict(self):
        return self.payload


class FakeQueryResult:
    """Minimal Spark SQL result stub."""

    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return [FakeRow(row) for row in self.rows]


class FakeSparkSession:
    """Fake Spark session keyed by query tag comments."""

    def __init__(self, rows_by_tag):
        self.rows_by_tag = rows_by_tag
        self.queries: list[str] = []

    def sql(self, query: str):
        self.queries.append(query)
        tag = query.split("*/", 1)[0].replace("/*", "").strip()
        rows = self.rows_by_tag.get(tag, [{"value": 0}])
        return FakeQueryResult(rows)


def _inventory_result():
    config = load_certification_config("50k")
    source_columns = {
        "player_master": (
            ("player_id", "string"),
            ("player_status", "string"),
            ("rating", "double"),
        ),
        "teams": (
            ("id", "string"),
            ("team_status", "string"),
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
        ),
        "matches": (
            ("id", "string"),
            ("winning_team_id", "string"),
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
                file_path=f"/Volumes/workspace/instructor_50k_raw/napa_files/{source['file_name']}",
                file_size=123,
                modification_ts=None,
                row_count={"player_master": 100, "teams": 40, "team_memberships": 80, "match_teams": 120, "matches": 60}[source["source_name"]],
                schema_hash=f"hash_{source['source_name']}",
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
        release_name="napa_50k",
        release_path="/Volumes/workspace/instructor_50k_raw/napa_files",
        source_mode="parquet",
        path_exists=True,
        expected_sources=tuple(expected_sources),
        discovered_files=tuple(
            type("F", (), {"file_name": source.file_name, "file_path": source.file_path, "file_size": 123, "modification_ts": None})()
            for source in loaded_sources
        ),
        loaded_sources=tuple(loaded_sources),
        manifest=None,
        rule_results=(),
    )


def _get_rule(results, rule_id):
    return next(rule for rule in results if rule.rule_id == rule_id)


def test_load_certification_snapshot_returns_none_when_missing(tmp_path) -> None:
    assert load_certification_snapshot(tmp_path / "missing.json") is None


def test_load_certification_snapshot_reads_json_payload(tmp_path) -> None:
    path = tmp_path / "snapshot.json"
    payload = {"release_name": "napa_50k", "team_count": 10}
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert load_certification_snapshot(path) == payload


def test_build_release_metrics_collects_summary_metrics() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "PLAYER_METRICS": [
                {"player_status_group": "ACTIVE", "player_count": 70, "average_rating": 4.1},
                {"player_status_group": "INACTIVE", "player_count": 30, "average_rating": 3.5},
            ],
            "PLAYER_RATED_COUNT": [{"value": 90}],
            "CANDIDATE_TEAM_COUNT": [{"value": 25}],
        }
    )

    metrics = build_release_metrics(spark, inventory_result)

    assert metrics.file_count == 5
    assert metrics.row_counts_by_source["player_master"] == 100
    assert metrics.player_status_distribution["ACTIVE"] == 70
    assert round(metrics.active_player_rate or 0.0, 4) == 0.7
    assert metrics.candidate_team_count == 25


def test_reconciliation_rules_pass_within_tolerance_and_snapshot_evidence() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "PLAYER_METRICS": [
                {"player_status_group": "ACTIVE", "player_count": 70, "average_rating": 4.0},
                {"player_status_group": "INACTIVE", "player_count": 30, "average_rating": 3.6},
            ],
            "PLAYER_RATED_COUNT": [{"value": 90}],
            "CANDIDATE_TEAM_COUNT": [{"value": 20}],
        }
    )
    metrics = build_release_metrics(spark, inventory_result)
    source_snapshot = {
        "release_name": "napa_50k",
        "file_row_counts": {
            "player_master.parquet": 100,
            "teams.parquet": 40,
            "matches.parquet": 60,
        },
        "schema_hashes": {
            "player_master.parquet": "hash_player_master",
            "teams.parquet": "hash_teams",
        },
        "player_status_distribution": {"ACTIVE": 69, "INACTIVE": 31},
        "team_count": 40,
        "match_count": 60,
        "average_rating": 3.9,
    }
    prior_snapshot = {
        "release_name": "napa_5k",
        "active_player_rate": 0.68,
        "candidate_team_count": 18,
    }
    cross_scale = [
        {
            "release_name": "napa_5k",
            "schema_hashes": {
                "player_master.parquet": "hash_player_master",
                "teams.parquet": "hash_teams",
            },
        }
    ]

    results = evaluate_reconciliation_and_regression_rules(
        config,
        metrics,
        source_snapshot=source_snapshot,
        prior_release_snapshot=prior_snapshot,
        cross_scale_snapshots=cross_scale,
    )

    assert _get_rule(results, "RAW_SOURCE_FILE_COUNT_RECONCILIATION").status == "PASS"
    assert _get_rule(results, "RAW_SOURCE_PLAYER_STATUS_RECONCILIATION").status == "PASS"
    assert _get_rule(results, "RAW_SCHEMA_CROSS_SCALE_CONSISTENCY").status == "PASS"
    assert _get_rule(results, "RAW_DISTRIBUTION_PRIOR_RELEASE_DRIFT").status == "PASS"
    assert _get_rule(results, "RAW_CANDIDATE_POOL_PRIOR_RELEASE_REGRESSION").status == "PASS"


def test_reconciliation_rules_fail_on_status_drift_team_drop_schema_drift_and_candidate_collapse() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "PLAYER_METRICS": [
                {"player_status_group": "ACTIVE", "player_count": 10, "average_rating": 4.0},
                {"player_status_group": "INACTIVE", "player_count": 90, "average_rating": 3.6},
            ],
            "PLAYER_RATED_COUNT": [{"value": 90}],
            "CANDIDATE_TEAM_COUNT": [{"value": 5}],
        }
    )
    metrics = build_release_metrics(spark, inventory_result)
    source_snapshot = {
        "release_name": "napa_50k",
        "file_row_counts": {
            "player_master.parquet": 100,
            "teams.parquet": 100,
            "matches.parquet": 60,
        },
        "schema_hashes": {
            "player_master.parquet": "hash_player_master",
            "teams.parquet": "hash_teams_old",
        },
        "player_status_distribution": {"ACTIVE": 80, "INACTIVE": 20},
        "team_count": 100,
        "match_count": 60,
        "average_rating": 5.0,
    }
    prior_snapshot = {
        "release_name": "napa_5k",
        "active_player_rate": 0.80,
        "candidate_team_count": 20,
    }
    cross_scale = [
        {
            "release_name": "napa_5k",
            "schema_hashes": {
                "player_master.parquet": "hash_player_master_old",
                "teams.parquet": "hash_teams_old",
            },
        }
    ]

    results = evaluate_reconciliation_and_regression_rules(
        config,
        metrics,
        source_snapshot=source_snapshot,
        prior_release_snapshot=prior_snapshot,
        cross_scale_snapshots=cross_scale,
    )

    assert _get_rule(results, "RAW_SOURCE_PLAYER_STATUS_RECONCILIATION").status == "FAIL"
    assert _get_rule(results, "RAW_SOURCE_TEAM_COUNT_RECONCILIATION").status == "FAIL"
    assert _get_rule(results, "RAW_SOURCE_RATING_RECONCILIATION").status == "FAIL"
    assert _get_rule(results, "RAW_SCHEMA_CROSS_SCALE_CONSISTENCY").status == "FAIL"
    assert _get_rule(results, "RAW_DISTRIBUTION_PRIOR_RELEASE_DRIFT").status == "FAIL"
    assert _get_rule(results, "RAW_CANDIDATE_POOL_PRIOR_RELEASE_REGRESSION").status == "FAIL"


def test_reconciliation_rules_warn_when_snapshot_inputs_are_missing() -> None:
    config, inventory_result = _inventory_result()
    spark = FakeSparkSession(
        {
            "PLAYER_METRICS": [
                {"player_status_group": "ACTIVE", "player_count": 70, "average_rating": 4.0},
                {"player_status_group": "INACTIVE", "player_count": 30, "average_rating": 3.6},
            ],
            "PLAYER_RATED_COUNT": [{"value": 90}],
            "CANDIDATE_TEAM_COUNT": [{"value": 20}],
        }
    )
    metrics = build_release_metrics(spark, inventory_result)

    results = evaluate_reconciliation_and_regression_rules(config, metrics)

    assert _get_rule(results, "RAW_SOURCE_FILE_COUNT_RECONCILIATION").status == "WARN"
    assert _get_rule(results, "RAW_SCHEMA_CROSS_SCALE_CONSISTENCY").status == "WARN"
    assert _get_rule(results, "RAW_DISTRIBUTION_PRIOR_RELEASE_DRIFT").status == "WARN"
