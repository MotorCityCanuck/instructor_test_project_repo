"""Tests for Silver-layer audit helpers."""

from pathlib import Path

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.environment import ReleaseEnvironment
from napa_pipeline.bronze_to_silver.silver_audit import (
    AuditFinding,
    SilverAuditReport,
    SilverTableAudit,
    build_null_profile_findings,
    run_silver_layer_audit,
)


class FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping

    def asDict(self, recursive: bool = True):
        return dict(self._mapping)


class FakeSchemaField:
    def __init__(self, name: str):
        self.name = name
        self.nullable = True
        self.dataType = type("DataType", (), {"simpleString": lambda self_: "string"})()


class FakeSchema:
    def __init__(self, field_names):
        self.fields = [FakeSchemaField(name) for name in field_names]


class FakeTable:
    def __init__(self, field_names):
        self.schema = FakeSchema(field_names)


class FakeCatalog:
    def __init__(self, existing_tables):
        self._existing_tables = set(existing_tables)

    def tableExists(self, table_name: str) -> bool:
        return table_name in self._existing_tables


class FakeSparkSession:
    def __init__(self, *, existing_tables, table_fields, query_results):
        self.catalog = FakeCatalog(existing_tables)
        self._table_fields = table_fields
        self._query_results = query_results

    def table(self, table_name: str):
        return FakeTable(self._table_fields[table_name])

    def sql(self, query: str):
        normalized = " ".join(query.split())
        for needle, rows in sorted(
            self._query_results.items(),
            key=lambda item: len(item[0]),
            reverse=True,
        ):
            if needle in normalized:
                return type("FakeResult", (), {"collect": lambda self_: [FakeRow(row) for row in rows]})()
        raise AssertionError(f"Unexpected SQL: {normalized}")


def _minimal_config() -> BronzeToSilverConfig:
    return BronzeToSilverConfig(
        data={
            "project": {
                "pipeline_name": "bronze_to_silver",
                "pipeline_version": "1.0.0",
                "processing_mode": "full_refresh",
            },
            "release": {"release_name": "napa_5k"},
            "runtime": {"catalog": "workspace"},
            "schemas": {
                "bronze": "instructor_5k_bronze",
                "silver": "instructor_5k_silver",
                "silver_reject": "instructor_5k_silver_reject",
                "operations": "instructor_ops",
            },
            "thresholds": {
                "expected_match_team_count": 2,
                "expected_match_team_player_count": 2,
            },
            "sources": {
                "player_master": {
                    "enabled": True,
                    "bronze_table": "player_master",
                    "source_file": "player_master.parquet",
                    "natural_key": ["player_id"],
                }
            },
            "silver_tables": {
                "players": {
                    "enabled": True,
                    "source": "player_master",
                    "target": "players",
                    "build_order": 30,
                    "primary_key": ["player_id"],
                }
            },
        },
        config_hash="config-hash",
        config_root=Path("."),
    )


def test_build_null_profile_findings_separates_required_from_optional_columns() -> None:
    findings, profiles = build_null_profile_findings(
        table_name="players",
        row_count=10,
        null_counts={"player_id": 1, "player_name": 4, "nickname": 2},
        required_columns=("player_id",),
        null_detail_limit=5,
    )

    assert any(finding.rule_id == "AUDIT_NULL_001" for finding in findings)
    warning = next(finding for finding in findings if finding.rule_id == "AUDIT_NULL_002")
    assert warning.sample_values[0].startswith("player_name=4")
    assert [profile.column_name for profile in profiles] == ["player_name", "nickname"]


def test_run_silver_layer_audit_detects_release_and_quality_status_anomalies() -> None:
    config = _minimal_config()
    environment = ReleaseEnvironment(
        catalog="workspace",
        bronze_schema="instructor_5k_bronze",
        silver_schema="instructor_5k_silver",
        silver_reject_schema="instructor_5k_silver_reject",
        operations_schema="instructor_ops",
    )
    table_fqn = "workspace.instructor_5k_silver.players"
    spark = FakeSparkSession(
        existing_tables={table_fqn},
        table_fields={
            table_fqn: [
                "player_id",
                "player_name",
                "_pipeline_run_id",
                "_pipeline_version",
                "_source_dataset",
                "_source_table",
                "_load_ts",
                "_record_hash",
                "_data_quality_status",
            ]
        },
        query_results={
            f"SELECT COUNT(*) AS value FROM {table_fqn}": [{"value": 2}],
            "SUM(CASE WHEN `player_id` IS NULL THEN 1 ELSE 0 END)": [
                {
                    "player_id": 0,
                    "player_name": 1,
                    "_pipeline_run_id": 0,
                    "_pipeline_version": 0,
                    "_source_dataset": 0,
                    "_source_table": 0,
                    "_load_ts": 0,
                    "_record_hash": 0,
                    "_data_quality_status": 0,
                }
            ],
            f"GROUP BY `player_id` HAVING COUNT(*) > 1": [{"duplicate_key_count": 0, "duplicate_row_count": 0}],
            f"FROM {table_fqn} WHERE `_source_dataset` IS NULL OR CAST(`_source_dataset` AS STRING) <> 'napa_5k'": [
                {"value": 1}
            ],
            "GROUP BY COALESCE(CAST(`_source_dataset` AS STRING), '<NULL>')": [
                {"sample_value": "napa_50k", "row_count": 1}
            ],
            f"FROM {table_fqn} WHERE `_source_table` IS NULL OR CAST(`_source_table` AS STRING) <> 'player_master'": [
                {"value": 0}
            ],
            f"FROM {table_fqn} WHERE `_data_quality_status` IS NULL OR UPPER(CAST(`_data_quality_status` AS STRING)) NOT IN ('ACCEPTED', 'WARNING', 'INFO')": [
                {"value": 1}
            ],
            "GROUP BY COALESCE(CAST(`_data_quality_status` AS STRING), '<NULL>')": [
                {"sample_value": "REJECTED", "row_count": 1}
            ],
        },
    )

    report = run_silver_layer_audit(
        spark,
        config,
        environment,
        include_cross_table=False,
    )

    assert report.checked_table_count == 1
    assert report.error_count == 2
    assert report.warning_count == 1
    findings = report.tables[0].findings
    assert any(finding.rule_id == "AUDIT_META_001" for finding in findings)
    assert any(finding.rule_id == "AUDIT_META_003" for finding in findings)
    assert any(finding.rule_id == "AUDIT_NULL_002" for finding in findings)


def test_audit_report_render_includes_cross_table_findings() -> None:
    report = SilverAuditReport(
        release_name="napa_5k",
        silver_schema_fqn="workspace.instructor_5k_silver",
        checked_table_count=1,
        expected_table_count=1,
        tables=(
            SilverTableAudit(
                table_name="players",
                table_fqn="workspace.instructor_5k_silver.players",
                exists=True,
                row_count=2,
                primary_key=("player_id",),
                findings=(),
                null_profiles=(),
            ),
        ),
        cross_table_findings=(
            AuditFinding(
                table_name="matches",
                rule_id="CROSS_MATCH_001",
                severity="WARNING",
                message="cross-table contract validation failed for `matches`",
                failed_row_count=3,
            ),
        ),
    )

    rendered = report.render_text()

    assert "Cross-table findings" in rendered
    assert "CROSS_MATCH_001" in rendered


def test_run_silver_layer_audit_treats_empty_competition_tables_as_errors() -> None:
    config = BronzeToSilverConfig(
        data={
            "project": {
                "pipeline_name": "bronze_to_silver",
                "pipeline_version": "1.0.0",
                "processing_mode": "full_refresh",
            },
            "release": {"release_name": "napa_5k"},
            "runtime": {"catalog": "workspace"},
            "schemas": {
                "bronze": "instructor_5k_bronze",
                "silver": "instructor_5k_silver",
                "silver_reject": "instructor_5k_silver_reject",
                "operations": "instructor_ops",
            },
            "thresholds": {
                "expected_match_team_count": 2,
                "expected_match_team_player_count": 2,
            },
            "sources": {
                "matches": {
                    "enabled": True,
                    "bronze_table": "matches",
                    "source_file": "matches.parquet",
                    "natural_key": ["id"],
                }
            },
            "silver_tables": {
                "matches": {
                    "enabled": True,
                    "source": "matches",
                    "target": "matches",
                    "build_order": 100,
                    "primary_key": ["match_id"],
                }
            },
        },
        config_hash="config-hash",
        config_root=Path("."),
    )
    environment = ReleaseEnvironment(
        catalog="workspace",
        bronze_schema="instructor_5k_bronze",
        silver_schema="instructor_5k_silver",
        silver_reject_schema="instructor_5k_silver_reject",
        operations_schema="instructor_ops",
    )
    table_fqn = "workspace.instructor_5k_silver.matches"
    spark = FakeSparkSession(
        existing_tables={table_fqn},
        table_fields={
            table_fqn: [
                "match_id",
                "_pipeline_run_id",
                "_pipeline_version",
                "_source_dataset",
                "_source_table",
                "_load_ts",
                "_record_hash",
                "_data_quality_status",
            ]
        },
        query_results={
            f"SELECT COUNT(*) AS value FROM {table_fqn}": [{"value": 0}],
            "SUM(CASE WHEN `match_id` IS NULL THEN 1 ELSE 0 END)": [
                {
                    "match_id": 0,
                    "_pipeline_run_id": 0,
                    "_pipeline_version": 0,
                    "_source_dataset": 0,
                    "_source_table": 0,
                    "_load_ts": 0,
                    "_record_hash": 0,
                    "_data_quality_status": 0,
                }
            ],
            f"GROUP BY `match_id` HAVING COUNT(*) > 1": [{"duplicate_key_count": 0, "duplicate_row_count": 0}],
            f"FROM {table_fqn} WHERE `_source_dataset` IS NULL OR CAST(`_source_dataset` AS STRING) <> 'napa_5k'": [
                {"value": 0}
            ],
            f"FROM {table_fqn} WHERE `_source_table` IS NULL OR CAST(`_source_table` AS STRING) <> 'matches'": [
                {"value": 0}
            ],
            f"FROM {table_fqn} WHERE `_data_quality_status` IS NULL OR UPPER(CAST(`_data_quality_status` AS STRING)) NOT IN ('ACCEPTED', 'WARNING', 'INFO')": [
                {"value": 0}
            ],
        },
    )

    report = run_silver_layer_audit(
        spark,
        config,
        environment,
        include_cross_table=False,
    )

    finding = next(f for f in report.findings if f.rule_id == "AUDIT_TABLE_002")
    assert finding.severity == "ERROR"
