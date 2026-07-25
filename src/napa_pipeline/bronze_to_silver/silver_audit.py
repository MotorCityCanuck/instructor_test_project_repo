"""Release-parameterized Silver-layer contract audit helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.cross_table import run_cross_table_validations_sql
from napa_pipeline.bronze_to_silver.environment import ReleaseEnvironment
from napa_pipeline.bronze_to_silver.metadata import STANDARD_METADATA_COLUMNS
from napa_pipeline.bronze_to_silver.operations import PipelineContext, create_pipeline_context


ALLOWED_QUALITY_STATUS_VALUES = ("ACCEPTED", "WARNING", "INFO")
DEFAULT_REQUIRED_NON_EMPTY_TABLES = (
    "matches",
    "match_teams",
    "match_team_players",
    "match_games",
)


@dataclass(frozen=True)
class AuditFinding:
    """One Silver-layer anomaly detected by the audit."""

    table_name: str
    rule_id: str
    severity: str
    message: str
    failed_row_count: int | None = None
    evaluated_row_count: int | None = None
    sample_values: tuple[str, ...] = ()


@dataclass(frozen=True)
class ColumnNullProfile:
    """Null-count profile for one Silver column."""

    column_name: str
    null_count: int
    null_pct: float


@dataclass(frozen=True)
class SilverTableAudit:
    """Audit result for one Silver table."""

    table_name: str
    table_fqn: str
    exists: bool
    row_count: int | None
    primary_key: tuple[str, ...]
    findings: tuple[AuditFinding, ...]
    null_profiles: tuple[ColumnNullProfile, ...]


@dataclass(frozen=True)
class SilverAuditReport:
    """End-to-end Silver-layer audit output."""

    release_name: str
    silver_schema_fqn: str
    checked_table_count: int
    expected_table_count: int
    tables: tuple[SilverTableAudit, ...]
    cross_table_findings: tuple[AuditFinding, ...]

    @property
    def findings(self) -> tuple[AuditFinding, ...]:
        findings: list[AuditFinding] = []
        for table_audit in self.tables:
            findings.extend(table_audit.findings)
        findings.extend(self.cross_table_findings)
        return tuple(findings)

    @property
    def error_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity.upper() != "WARNING")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity.upper() == "WARNING")

    @property
    def anomaly_count(self) -> int:
        return len(self.findings)

    def render_text(self, *, null_detail_limit: int = 8) -> str:
        """Render the audit report as readable plain text."""
        lines = [
            "Silver Layer Audit",
            f"Release: {self.release_name}",
            f"Schema: {self.silver_schema_fqn}",
            f"Configured tables checked: {self.checked_table_count}/{self.expected_table_count}",
            f"Findings: {self.anomaly_count} total ({self.error_count} errors, {self.warning_count} warnings)",
            "",
        ]

        for table_audit in self.tables:
            row_count_text = "missing" if table_audit.row_count is None else str(table_audit.row_count)
            lines.append(f"Table `{table_audit.table_name}`: rows={row_count_text}")
            if not table_audit.findings:
                lines.append("- no anomalies detected")
            else:
                for finding in table_audit.findings:
                    detail = f"{finding.severity} {finding.rule_id}: {finding.message}"
                    if finding.failed_row_count is not None:
                        detail += f" [failed_rows={finding.failed_row_count}]"
                    if finding.sample_values:
                        detail += f" [samples={', '.join(finding.sample_values)}]"
                    lines.append(f"- {detail}")
            if table_audit.null_profiles:
                lines.append("- null-bearing columns:")
                for profile in table_audit.null_profiles[:null_detail_limit]:
                    lines.append(
                        f"  {profile.column_name}={profile.null_count} ({profile.null_pct:.2f}%)"
                    )
                if len(table_audit.null_profiles) > null_detail_limit:
                    lines.append(
                        f"  ... {len(table_audit.null_profiles) - null_detail_limit} additional columns omitted"
                    )
            lines.append("")

        if self.cross_table_findings:
            lines.append("Cross-table findings")
            for finding in self.cross_table_findings:
                detail = f"{finding.severity} {finding.rule_id} on `{finding.table_name}`: {finding.message}"
                if finding.failed_row_count is not None:
                    detail += f" [failed_rows={finding.failed_row_count}]"
                if finding.sample_values:
                    detail += f" [samples={', '.join(finding.sample_values)}]"
                lines.append(f"- {detail}")
            lines.append("")

        return "\n".join(lines).rstrip()


def run_silver_layer_audit(
    spark: Any,
    config: BronzeToSilverConfig,
    environment: ReleaseEnvironment,
    *,
    sample_limit: int = 10,
    include_cross_table: bool = True,
) -> SilverAuditReport:
    """Audit every configured Silver table for nulls and contract anomalies."""
    silver_schema_fqn = f"{environment.catalog}.{environment.silver_schema}"
    table_audits = [
        _audit_single_table(
            spark,
            config,
            environment,
            table_config=table_config,
            sample_limit=sample_limit,
            required_non_empty_tables=_required_non_empty_tables(config),
        )
        for table_config in config.silver_tables_in_build_order
    ]

    cross_table_findings: tuple[AuditFinding, ...] = ()
    if include_cross_table:
        context = create_pipeline_context(config, environment)
        cross_table_findings = _convert_cross_table_findings(
            run_cross_table_validations_sql(
                spark,
                context,
                environment,
                expected_match_team_count=int(config.data["thresholds"]["expected_match_team_count"]),
                expected_match_team_player_count=int(
                    config.data["thresholds"]["expected_match_team_player_count"]
                ),
                duplicate_active_team_pair_severity=str(
                    config.data["thresholds"].get("duplicate_active_team_pair_severity", "WARNING")
                ),
            )
        )

    return SilverAuditReport(
        release_name=config.release_name,
        silver_schema_fqn=silver_schema_fqn,
        checked_table_count=len(table_audits),
        expected_table_count=len(config.silver_tables_in_build_order),
        tables=tuple(table_audits),
        cross_table_findings=cross_table_findings,
    )


def build_null_profile_findings(
    *,
    table_name: str,
    row_count: int,
    null_counts: dict[str, int],
    required_columns: Iterable[str],
    null_detail_limit: int,
) -> tuple[tuple[AuditFinding, ...], tuple[ColumnNullProfile, ...]]:
    """Classify required-column null failures and optional-column null warnings."""
    required_set = {column for column in required_columns}
    findings: list[AuditFinding] = []
    optional_profiles: list[ColumnNullProfile] = []

    for column_name, null_count in sorted(null_counts.items()):
        if null_count <= 0:
            continue
        null_pct = ((null_count / row_count) * 100.0) if row_count else 0.0
        if column_name in required_set:
            findings.append(
                AuditFinding(
                    table_name=table_name,
                    rule_id="AUDIT_NULL_001",
                    severity="ERROR",
                    message=f"required contract column `{column_name}` contains null values",
                    failed_row_count=null_count,
                    evaluated_row_count=row_count,
                    sample_values=(column_name,),
                )
            )
        else:
            optional_profiles.append(
                ColumnNullProfile(
                    column_name=column_name,
                    null_count=int(null_count),
                    null_pct=null_pct,
                )
            )

    optional_profiles.sort(key=lambda item: (-item.null_count, item.column_name))
    if optional_profiles:
        sample_columns = tuple(
            f"{profile.column_name}={profile.null_count} ({profile.null_pct:.2f}%)"
            for profile in optional_profiles[:null_detail_limit]
        )
        findings.append(
            AuditFinding(
                table_name=table_name,
                rule_id="AUDIT_NULL_002",
                severity="WARNING",
                message="non-required columns contain missing values",
                failed_row_count=sum(profile.null_count for profile in optional_profiles),
                evaluated_row_count=row_count,
                sample_values=sample_columns,
            )
        )

    return tuple(findings), tuple(optional_profiles)


def _audit_single_table(
    spark: Any,
    config: BronzeToSilverConfig,
    environment: ReleaseEnvironment,
    *,
    table_config: dict[str, Any],
    sample_limit: int,
    required_non_empty_tables: tuple[str, ...],
) -> SilverTableAudit:
    table_name = str(table_config["target"])
    table_fqn = f"{environment.catalog}.{environment.silver_schema}.{table_name}"
    primary_key = tuple(str(column) for column in table_config.get("primary_key", []))
    findings: list[AuditFinding] = []

    if not spark.catalog.tableExists(table_fqn):
        findings.append(
            AuditFinding(
                table_name=table_name,
                rule_id="AUDIT_TABLE_001",
                severity="ERROR",
                message=f"configured Silver table `{table_fqn}` is missing",
            )
        )
        return SilverTableAudit(
            table_name=table_name,
            table_fqn=table_fqn,
            exists=False,
            row_count=None,
            primary_key=primary_key,
            findings=tuple(findings),
            null_profiles=(),
        )

    schema = spark.table(table_fqn).schema
    columns = [field.name for field in getattr(schema, "fields", [])]
    row_count = _scalar_int(spark, f"SELECT COUNT(*) AS value FROM {table_fqn}")

    missing_primary_key = [column for column in primary_key if column not in columns]
    if missing_primary_key:
        findings.append(
            AuditFinding(
                table_name=table_name,
                rule_id="AUDIT_SCHEMA_001",
                severity="ERROR",
                message="primary-key columns are missing from the published Silver table",
                sample_values=tuple(missing_primary_key),
            )
        )

    missing_metadata = [column for column in STANDARD_METADATA_COLUMNS if column not in columns]
    if missing_metadata:
        findings.append(
            AuditFinding(
                table_name=table_name,
                rule_id="AUDIT_SCHEMA_002",
                severity="ERROR",
                message="standard Silver metadata columns are missing",
                sample_values=tuple(missing_metadata),
            )
        )

    if row_count == 0:
        findings.append(
            AuditFinding(
                table_name=table_name,
                rule_id="AUDIT_TABLE_002",
                severity="ERROR" if table_name in required_non_empty_tables else "WARNING",
                message="published Silver table is empty",
                failed_row_count=0,
                evaluated_row_count=0,
            )
        )

    null_counts = _null_counts(
        spark,
        table_fqn,
        columns,
    )
    null_findings, optional_profiles = build_null_profile_findings(
        table_name=table_name,
        row_count=row_count,
        null_counts=null_counts,
        required_columns=tuple(primary_key)
        + STANDARD_METADATA_COLUMNS
        + tuple(str(column) for column in table_config.get("required_contract_columns", ())),
        null_detail_limit=sample_limit,
    )
    findings.extend(null_findings)

    if all(column in columns for column in primary_key):
        duplicate_stats = _collect_first_row(
            spark,
            _build_duplicate_key_sql(table_fqn, primary_key),
        )
        duplicate_group_count = int(duplicate_stats.get("duplicate_key_count") or 0)
        duplicate_row_count = int(duplicate_stats.get("duplicate_row_count") or 0)
        if duplicate_group_count:
            findings.append(
                AuditFinding(
                    table_name=table_name,
                    rule_id="AUDIT_KEY_001",
                    severity="ERROR",
                    message="primary key contains duplicate business keys",
                    failed_row_count=duplicate_row_count,
                    evaluated_row_count=row_count,
                    sample_values=_duplicate_key_samples(
                        spark,
                        table_fqn,
                        primary_key,
                        sample_limit,
                    ),
                )
            )

    findings.extend(
        _metadata_value_findings(
            spark,
            config,
            table_name=table_name,
            table_fqn=table_fqn,
            columns=columns,
            row_count=row_count,
            source_table=str(
                config.enabled_sources[str(table_config["source"])]["bronze_table"]
            ),
            sample_limit=sample_limit,
        )
    )

    return SilverTableAudit(
        table_name=table_name,
        table_fqn=table_fqn,
        exists=True,
        row_count=row_count,
        primary_key=primary_key,
        findings=tuple(findings),
        null_profiles=optional_profiles,
    )


def _metadata_value_findings(
    spark: Any,
    config: BronzeToSilverConfig,
    *,
    table_name: str,
    table_fqn: str,
    columns: list[str],
    row_count: int,
    source_table: str,
    sample_limit: int,
) -> list[AuditFinding]:
    findings: list[AuditFinding] = []

    if "_source_dataset" in columns:
        mismatch_count = _scalar_int(
            spark,
            f"""
SELECT COUNT(*) AS value
FROM {table_fqn}
WHERE `_source_dataset` IS NULL
   OR CAST(`_source_dataset` AS STRING) <> {_sql_literal(config.release_name)}
""".strip(),
        )
        if mismatch_count:
            findings.append(
                AuditFinding(
                    table_name=table_name,
                    rule_id="AUDIT_META_001",
                    severity="ERROR",
                    message="`_source_dataset` contains values outside the selected release",
                    failed_row_count=mismatch_count,
                    evaluated_row_count=row_count,
                    sample_values=_sample_grouped_values(
                        spark,
                        table_fqn,
                        column_name="_source_dataset",
                        where_clause=(
                            "`_source_dataset` IS NULL OR "
                            f"CAST(`_source_dataset` AS STRING) <> {_sql_literal(config.release_name)}"
                        ),
                        sample_limit=sample_limit,
                    ),
                )
            )

    if "_source_table" in columns:
        mismatch_count = _scalar_int(
            spark,
            f"""
SELECT COUNT(*) AS value
FROM {table_fqn}
WHERE `_source_table` IS NULL
   OR CAST(`_source_table` AS STRING) <> {_sql_literal(source_table)}
""".strip(),
        )
        if mismatch_count:
            findings.append(
                AuditFinding(
                    table_name=table_name,
                    rule_id="AUDIT_META_002",
                    severity="ERROR",
                    message="`_source_table` does not match the configured Bronze source",
                    failed_row_count=mismatch_count,
                    evaluated_row_count=row_count,
                    sample_values=_sample_grouped_values(
                        spark,
                        table_fqn,
                        column_name="_source_table",
                        where_clause=(
                            "`_source_table` IS NULL OR "
                            f"CAST(`_source_table` AS STRING) <> {_sql_literal(source_table)}"
                        ),
                        sample_limit=sample_limit,
                    ),
                )
            )

    if "_data_quality_status" in columns:
        allowed_values_sql = ", ".join(_sql_literal(value) for value in ALLOWED_QUALITY_STATUS_VALUES)
        mismatch_count = _scalar_int(
            spark,
            f"""
SELECT COUNT(*) AS value
FROM {table_fqn}
WHERE `_data_quality_status` IS NULL
   OR UPPER(CAST(`_data_quality_status` AS STRING)) NOT IN ({allowed_values_sql})
""".strip(),
        )
        if mismatch_count:
            findings.append(
                AuditFinding(
                    table_name=table_name,
                    rule_id="AUDIT_META_003",
                    severity="ERROR",
                    message="`_data_quality_status` contains unexpected values in the published Silver table",
                    failed_row_count=mismatch_count,
                    evaluated_row_count=row_count,
                    sample_values=_sample_grouped_values(
                        spark,
                        table_fqn,
                        column_name="_data_quality_status",
                        where_clause=(
                            "`_data_quality_status` IS NULL OR "
                            f"UPPER(CAST(`_data_quality_status` AS STRING)) NOT IN ({allowed_values_sql})"
                        ),
                        sample_limit=sample_limit,
                    ),
                )
            )

    return findings


def _convert_cross_table_findings(result: Any) -> tuple[AuditFinding, ...]:
    findings = []
    for row in getattr(result, "quality_results", ()):
        failed_row_count = int(row.get("failed_row_count") or 0)
        if failed_row_count <= 0:
            continue
        findings.append(
            AuditFinding(
                table_name=str(row["target_table"]),
                rule_id=str(row["rule_id"]),
                severity=str(row["severity"]),
                message=f"cross-table contract validation failed for `{row['target_table']}`",
                failed_row_count=failed_row_count,
                evaluated_row_count=(
                    int(row["evaluated_row_count"])
                    if row.get("evaluated_row_count") is not None
                    else None
                ),
                sample_values=tuple(str(value) for value in (row.get("sample_business_keys") or [])),
            )
        )
    return tuple(findings)


def _null_counts(
    spark: Any,
    table_fqn: str,
    columns: list[str],
) -> dict[str, int]:
    if not columns:
        return {}
    expressions = ",\n    ".join(
        f"SUM(CASE WHEN `{column}` IS NULL THEN 1 ELSE 0 END) AS `{column}`"
        for column in columns
    )
    row = _collect_first_row(
        spark,
        f"""
SELECT
    {expressions}
FROM {table_fqn}
""".strip(),
    )
    return {column: int(row.get(column) or 0) for column in columns}


def _duplicate_key_samples(
    spark: Any,
    table_fqn: str,
    primary_key: tuple[str, ...],
    sample_limit: int,
) -> tuple[str, ...]:
    if not primary_key:
        return ()
    key_string = ", ".join(f"CAST(`{column}` AS STRING)" for column in primary_key)
    rows = _collect_rows(
        spark,
        f"""
SELECT
    CONCAT_WS('||', {key_string}) AS sample_business_key
FROM (
    SELECT {", ".join(f"`{column}`" for column in primary_key)}
    FROM {table_fqn}
    GROUP BY {", ".join(f"`{column}`" for column in primary_key)}
    HAVING COUNT(*) > 1
) duplicate_keys
LIMIT {int(sample_limit)}
""".strip(),
    )
    return tuple(str(row.get("sample_business_key")) for row in rows if row.get("sample_business_key"))


def _sample_grouped_values(
    spark: Any,
    table_fqn: str,
    *,
    column_name: str,
    where_clause: str,
    sample_limit: int,
) -> tuple[str, ...]:
    rows = _collect_rows(
        spark,
        f"""
SELECT
    COALESCE(CAST(`{column_name}` AS STRING), '<NULL>') AS sample_value,
    COUNT(*) AS row_count
FROM {table_fqn}
WHERE {where_clause}
GROUP BY COALESCE(CAST(`{column_name}` AS STRING), '<NULL>')
ORDER BY row_count DESC, sample_value ASC
LIMIT {int(sample_limit)}
""".strip(),
    )
    return tuple(
        f"{row['sample_value']} ({row['row_count']})"
        for row in rows
        if row.get("sample_value") is not None
    )


def _build_duplicate_key_sql(table_fqn: str, primary_key: tuple[str, ...]) -> str:
    key_columns = ", ".join(f"`{column}`" for column in primary_key)
    return f"""
SELECT
    COUNT(*) AS duplicate_key_count,
    COALESCE(SUM(group_count), 0) AS duplicate_row_count
FROM (
    SELECT COUNT(*) AS group_count
    FROM {table_fqn}
    GROUP BY {key_columns}
    HAVING COUNT(*) > 1
) duplicate_groups
""".strip()


def _collect_first_row(spark: Any, sql_text: str) -> dict[str, Any]:
    rows = _collect_rows(spark, sql_text)
    return rows[0] if rows else {}


def _collect_rows(spark: Any, sql_text: str) -> list[dict[str, Any]]:
    rows = spark.sql(sql_text).collect()
    return [row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row) for row in rows]


def _scalar_int(spark: Any, sql_text: str) -> int:
    row = _collect_first_row(spark, sql_text)
    return int(row.get("value") or 0)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _required_non_empty_tables(config: BronzeToSilverConfig) -> tuple[str, ...]:
    configured = config.data.get("thresholds", {}).get("required_non_empty_tables")
    if isinstance(configured, (list, tuple)) and configured:
        return tuple(str(value) for value in configured)
    return DEFAULT_REQUIRED_NON_EMPTY_TABLES
