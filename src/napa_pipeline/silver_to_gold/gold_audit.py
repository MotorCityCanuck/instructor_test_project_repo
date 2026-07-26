"""Standalone descriptive and anomaly-focused audit for published Gold tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from napa_pipeline.silver_to_gold.config import SilverToGoldConfig
from napa_pipeline.silver_to_gold.environment import ReleaseEnvironment
from napa_pipeline.silver_to_gold.io import get_gold_target_table_fqn
from napa_pipeline.silver_to_gold.operations import (
    COLUMN_PROFILE_RESULTS_TABLE,
    QUALITY_RESULTS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    TABLE_PROFILE_RESULTS_TABLE,
    append_records,
    build_quality_result_record,
    build_reconciliation_record,
    get_operations_table_fqn,
    utc_now,
    PipelineContext,
)
from napa_pipeline.silver_to_gold.workflow import MATERIALIZED_GOLD_TARGET_TABLES


class GoldAuditValidationError(RuntimeError):
    """Raised when one or more Gold audit checks fail."""


@dataclass(frozen=True)
class GoldAuditSummary:
    """Summary of a completed standalone Gold-layer audit run."""

    table_profile_results_fqn: str
    column_profile_results_fqn: str
    quality_results_fqn: str
    reconciliation_results_fqn: str
    audited_table_count: int
    table_profile_row_count: int
    column_profile_row_count: int
    quality_record_count: int
    reconciliation_record_count: int
    warning_count: int
    critical_failure_count: int


@dataclass(frozen=True)
class _GoldTableSpec:
    table_name: str
    build_order: int
    build_stage: str
    primary_key: tuple[str, ...]


@dataclass(frozen=True)
class _GoldTableProfile:
    table_name: str
    table_fqn: str
    build_order: int
    build_stage: str
    primary_key: tuple[str, ...]
    row_count: int
    column_count: int
    null_primary_key_row_count: int
    duplicate_primary_key_group_count: int
    duplicate_primary_key_row_count: int
    null_primary_key_sample_keys: tuple[str, ...]
    duplicate_primary_key_sample_keys: tuple[str, ...]
    empty_table_flag: bool
    distinct_analysis_as_of_date_count: int | None
    distinct_scoring_scenario_count: int | None
    anomaly_count: int
    warning_count: int
    status: str


@dataclass(frozen=True)
class _ExpectedReconciliation:
    name: str
    source_count: int
    accepted_count: int


def publish_gold_layer_audit(
    spark: Any,
    context: PipelineContext,
    config: SilverToGoldConfig,
    environment: ReleaseEnvironment,
) -> GoldAuditSummary:
    """Profile materialized Gold outputs, persist diagnostics, and fail on critical anomalies."""
    table_specs = _get_materialized_gold_table_specs(config)
    profiled_ts = utc_now()
    table_profile_records: list[dict[str, Any]] = []
    column_profile_records: list[dict[str, Any]] = []
    quality_records: list[dict[str, Any]] = []
    row_counts_by_table: dict[str, int] = {}
    warning_count = 0
    critical_failure_count = 0

    for spec in table_specs:
        table_fqn = get_gold_target_table_fqn(environment, spec.table_name)
        if not spark.catalog.tableExists(table_fqn):
            critical_failure_count += 1
            quality_records.append(
                build_quality_result_record(
                    context,
                    target_table=spec.table_name,
                    rule_id="table_exists",
                    rule_category="inventory",
                    severity="ERROR",
                    status="FAILED",
                    evaluated_row_count=None,
                    failed_row_count=1,
                    failure_pct=100.0,
                    sample_keys=[table_fqn],
                    evaluated_ts=profiled_ts,
                )
            )
            continue

        column_profiles = _profile_gold_table_columns(spark, table_fqn, spec.table_name, profiled_ts)
        column_profile_records.extend(
            _build_column_profile_records(
                context,
                spec.table_name,
                column_profiles,
                profiled_ts=profiled_ts,
            )
        )

        profile = _profile_gold_table(
            spark,
            table_fqn=table_fqn,
            spec=spec,
            profiled_ts=profiled_ts,
        )
        row_counts_by_table[spec.table_name] = profile.row_count

        table_anomalies = evaluate_table_profile_anomalies(
            profile,
            column_profiles=column_profiles,
            expected_analysis_as_of_date=str(context.analysis_as_of_date),
            expected_scoring_scenario=context.scoring_scenario,
            profiled_ts=profiled_ts,
            context=context,
        )
        quality_records.extend(table_anomalies)
        warning_count += sum(1 for record in table_anomalies if record["severity"] == "WARNING")
        critical_failure_count += sum(
            1
            for record in table_anomalies
            if record["severity"] == "ERROR" and record["status"] == "FAILED"
        )

        table_profile_records.append(
            _build_table_profile_record(
                context,
                profile,
                profiled_ts=profiled_ts,
            )
        )

    reconciliation_records = [
        build_reconciliation_record(
            context,
            reconciliation_name=rule.name,
            source_count=rule.source_count,
            accepted_count=rule.accepted_count,
            excluded_count=0,
            evaluated_ts=profiled_ts,
        )
        for rule in build_expected_reconciliations(
            row_counts_by_table,
            evidence_window_count=_get_player_performance_window_count(config),
            sensitivity_scenario_count=len(config.data["sensitivity"]["scenarios"]),
        )
    ]
    critical_failure_count += sum(
        1 for record in reconciliation_records if record["status"] == "FAILED"
    )

    table_profile_results_fqn = get_operations_table_fqn(context, TABLE_PROFILE_RESULTS_TABLE)
    column_profile_results_fqn = get_operations_table_fqn(context, COLUMN_PROFILE_RESULTS_TABLE)
    quality_results_fqn = get_operations_table_fqn(context, QUALITY_RESULTS_TABLE)
    reconciliation_results_fqn = get_operations_table_fqn(context, RECONCILIATION_RESULTS_TABLE)

    append_records(spark, table_profile_results_fqn, table_profile_records)
    append_records(spark, column_profile_results_fqn, column_profile_records)
    append_records(spark, quality_results_fqn, quality_records)
    append_records(spark, reconciliation_results_fqn, reconciliation_records)

    if critical_failure_count > 0:
        failure_summary = build_gold_audit_failure_summary(
            quality_records,
            reconciliation_records,
        )
        raise GoldAuditValidationError(
            "Gold audit detected critical anomalies. "
            f"critical_failures={critical_failure_count}, warnings={warning_count}.\n"
            f"{failure_summary}"
        )

    return GoldAuditSummary(
        table_profile_results_fqn=table_profile_results_fqn,
        column_profile_results_fqn=column_profile_results_fqn,
        quality_results_fqn=quality_results_fqn,
        reconciliation_results_fqn=reconciliation_results_fqn,
        audited_table_count=len(table_profile_records),
        table_profile_row_count=len(table_profile_records),
        column_profile_row_count=len(column_profile_records),
        quality_record_count=len(quality_records),
        reconciliation_record_count=len(reconciliation_records),
        warning_count=warning_count,
        critical_failure_count=critical_failure_count,
    )


def build_gold_audit_failure_summary(
    quality_records: list[dict[str, Any]],
    reconciliation_records: list[dict[str, Any]],
    *,
    limit: int = 20,
) -> str:
    """Return a compact human-readable summary of failed audit records."""
    failed_quality_records = [
        record
        for record in quality_records
        if record.get("status") == "FAILED" and record.get("severity") == "ERROR"
    ]
    failed_reconciliation_records = [
        record for record in reconciliation_records if record.get("status") == "FAILED"
    ]
    lines = ["Failed audit details:"]

    if failed_quality_records:
        lines.append("Quality failures:")
        for record in failed_quality_records[:limit]:
            lines.append(
                "  - "
                f"{record.get('target_table')}.{record.get('rule_id')}: "
                f"failed_rows={record.get('failed_row_count')}, "
                f"failure_pct={record.get('failure_pct')}, "
                f"sample_keys={record.get('sample_keys')}"
            )
        if len(failed_quality_records) > limit:
            lines.append(
                f"  - ... {len(failed_quality_records) - limit} additional quality failures"
            )
    else:
        lines.append("Quality failures: none")

    if failed_reconciliation_records:
        lines.append("Reconciliation failures:")
        for record in failed_reconciliation_records[:limit]:
            lines.append(
                "  - "
                f"{record.get('reconciliation_name')}: "
                f"source_count={record.get('source_count')}, "
                f"accepted_count={record.get('accepted_count')}, "
                f"difference={record.get('difference')}"
            )
        if len(failed_reconciliation_records) > limit:
            lines.append(
                "  - ... "
                f"{len(failed_reconciliation_records) - limit} additional reconciliation failures"
            )
    else:
        lines.append("Reconciliation failures: none")

    return "\n".join(lines)


def build_expected_reconciliations(
    row_counts_by_table: dict[str, int],
    *,
    evidence_window_count: int,
    sensitivity_scenario_count: int,
) -> list[_ExpectedReconciliation]:
    """Return rule-based cross-table row-balance expectations."""
    rules: list[_ExpectedReconciliation] = []
    match_sides = row_counts_by_table.get("competition_match_sides")
    player_matches = row_counts_by_table.get("competition_player_matches")
    if match_sides is not None and player_matches is not None:
        rules.append(
            _ExpectedReconciliation(
                name="competition_player_matches_per_side",
                source_count=match_sides * 2,
                accepted_count=player_matches,
            )
        )

    player_development = row_counts_by_table.get("player_development_features")
    player_performance = row_counts_by_table.get("player_performance_features")
    if player_development is not None and player_performance is not None:
        rules.append(
            _ExpectedReconciliation(
                name="player_performance_feature_window_balance",
                source_count=player_development * evidence_window_count,
                accepted_count=player_performance,
            )
        )

    recommendations = row_counts_by_table.get("olympic_team_recommendations")
    explanations = row_counts_by_table.get("recommendation_explanations")
    if recommendations is not None and explanations is not None:
        rules.append(
            _ExpectedReconciliation(
                name="recommendation_explanation_coverage",
                source_count=recommendations,
                accepted_count=explanations,
            )
        )

    sensitivity = row_counts_by_table.get("selection_sensitivity_results")
    if recommendations is not None and sensitivity is not None:
        rules.append(
            _ExpectedReconciliation(
                name="selection_sensitivity_scenario_coverage",
                source_count=recommendations * sensitivity_scenario_count,
                accepted_count=sensitivity,
            )
        )

    return rules


def evaluate_table_profile_anomalies(
    profile: _GoldTableProfile,
    *,
    column_profiles: dict[str, dict[str, Any]],
    expected_analysis_as_of_date: str,
    expected_scoring_scenario: str,
    profiled_ts: datetime,
    context: PipelineContext,
) -> list[dict[str, Any]]:
    """Return explicit anomaly records for one profiled Gold table."""
    records: list[dict[str, Any]] = []
    if profile.row_count == 0:
        records.append(
            build_quality_result_record(
                context,
                target_table=profile.table_name,
                rule_id="non_empty_table",
                rule_category="content",
                severity="WARNING",
                status="FAILED",
                evaluated_row_count=profile.row_count,
                failed_row_count=profile.row_count,
                failure_pct=100.0,
                evaluated_ts=profiled_ts,
            )
        )

    if profile.null_primary_key_row_count > 0:
        records.append(
            build_quality_result_record(
                context,
                target_table=profile.table_name,
                rule_id="primary_key_not_null",
                rule_category="primary_key",
                severity="ERROR",
                status="FAILED",
                evaluated_row_count=profile.row_count,
                failed_row_count=profile.null_primary_key_row_count,
                failure_pct=_calculate_failure_pct(profile.null_primary_key_row_count, profile.row_count),
                sample_keys=list(profile.null_primary_key_sample_keys),
                evaluated_ts=profiled_ts,
            )
        )

    if profile.duplicate_primary_key_group_count > 0:
        records.append(
            build_quality_result_record(
                context,
                target_table=profile.table_name,
                rule_id="primary_key_unique",
                rule_category="primary_key",
                severity="ERROR",
                status="FAILED",
                evaluated_row_count=profile.row_count,
                failed_row_count=profile.duplicate_primary_key_row_count,
                failure_pct=_calculate_failure_pct(
                    profile.duplicate_primary_key_row_count,
                    profile.row_count,
                ),
                sample_keys=list(profile.duplicate_primary_key_sample_keys),
                evaluated_ts=profiled_ts,
            )
        )

    if "analysis_as_of_date" in column_profiles:
        analysis_profile = column_profiles["analysis_as_of_date"]
        actual_min = analysis_profile["min_value"]
        actual_max = analysis_profile["max_value"]
        distinct_count = profile.distinct_analysis_as_of_date_count or 0
        if distinct_count != 1 or actual_min != expected_analysis_as_of_date or actual_max != expected_analysis_as_of_date:
            records.append(
                build_quality_result_record(
                    context,
                    target_table=profile.table_name,
                    rule_id="analysis_as_of_date_alignment",
                    rule_category="consistency",
                    severity="ERROR",
                    status="FAILED",
                    evaluated_row_count=profile.row_count,
                    failed_row_count=profile.row_count,
                    failure_pct=100.0 if profile.row_count else 0.0,
                    sample_keys=[
                        f"min={actual_min}",
                        f"max={actual_max}",
                        f"expected={expected_analysis_as_of_date}",
                    ],
                    evaluated_ts=profiled_ts,
                )
            )

    if "scoring_scenario" in column_profiles:
        scenario_profile = column_profiles["scoring_scenario"]
        actual_min = scenario_profile["min_value"]
        actual_max = scenario_profile["max_value"]
        distinct_count = profile.distinct_scoring_scenario_count or 0
        if distinct_count != 1 or actual_min != expected_scoring_scenario or actual_max != expected_scoring_scenario:
            records.append(
                build_quality_result_record(
                    context,
                    target_table=profile.table_name,
                    rule_id="scoring_scenario_alignment",
                    rule_category="consistency",
                    severity="ERROR",
                    status="FAILED",
                    evaluated_row_count=profile.row_count,
                    failed_row_count=profile.row_count,
                    failure_pct=100.0 if profile.row_count else 0.0,
                    sample_keys=[
                        f"min={actual_min}",
                        f"max={actual_max}",
                        f"expected={expected_scoring_scenario}",
                    ],
                    evaluated_ts=profiled_ts,
                )
            )

    return records


def _get_materialized_gold_table_specs(config: SilverToGoldConfig) -> list[_GoldTableSpec]:
    """Return the current workflow's materialized Gold targets in build order."""
    specs: list[_GoldTableSpec] = []
    for table_name in MATERIALIZED_GOLD_TARGET_TABLES:
        table_config = config.data["gold_tables"][table_name]
        specs.append(
            _GoldTableSpec(
                table_name=table_name,
                build_order=int(table_config["build_order"]),
                build_stage=str(table_config["stage"]),
                primary_key=tuple(str(column) for column in table_config["primary_key"]),
            )
        )
    return specs


def _profile_gold_table_columns(
    spark: Any,
    table_fqn: str,
    table_name: str,
    profiled_ts: datetime,
) -> dict[str, dict[str, Any]]:
    """Return one profile mapping per column for one Gold table."""
    del profiled_ts
    dataframe = spark.table(table_fqn)
    schema = dataframe.schema
    expressions = ["COUNT(*) AS row_count"]
    column_names = [field.name for field in schema.fields]

    for index, field in enumerate(schema.fields):
        quoted = _quote_identifier(field.name)
        expressions.extend(
            [
                f"SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS c{index}_null_count",
                f"APPROX_COUNT_DISTINCT(CAST({quoted} AS STRING)) AS c{index}_approx_distinct_count",
                f"MIN(CAST({quoted} AS STRING)) AS c{index}_min_value",
                f"MAX(CAST({quoted} AS STRING)) AS c{index}_max_value",
            ]
        )

    aggregate_row = spark.sql(
        f"SELECT {', '.join(expressions)} FROM {table_fqn}"
    ).collect()[0]
    aggregate_mapping = aggregate_row.asDict(recursive=True) if hasattr(aggregate_row, "asDict") else dict(aggregate_row)
    row_count = int(aggregate_mapping["row_count"] or 0)

    profiles: dict[str, dict[str, Any]] = {}
    for index, field in enumerate(schema.fields):
        null_count = int(aggregate_mapping.get(f"c{index}_null_count") or 0)
        profiles[column_names[index]] = {
            "column_name": field.name,
            "data_type": field.dataType.simpleString() if hasattr(field.dataType, "simpleString") else str(field.dataType),
            "nullable": bool(field.nullable),
            "row_count": row_count,
            "non_null_count": row_count - null_count,
            "null_count": null_count,
            "null_pct": _calculate_failure_pct(null_count, row_count),
            "approx_distinct_count": int(
                aggregate_mapping.get(f"c{index}_approx_distinct_count") or 0
            ),
            "min_value": aggregate_mapping.get(f"c{index}_min_value"),
            "max_value": aggregate_mapping.get(f"c{index}_max_value"),
        }
    return profiles


def _profile_gold_table(
    spark: Any,
    *,
    table_fqn: str,
    spec: _GoldTableSpec,
    profiled_ts: datetime,
) -> _GoldTableProfile:
    """Return one table-level profile for a Gold target."""
    del profiled_ts
    dataframe = spark.table(table_fqn)
    schema = dataframe.schema
    row_count = int(dataframe.count())
    column_names = {field.name for field in schema.fields}

    null_primary_key_row_count = 0
    duplicate_primary_key_group_count = 0
    duplicate_primary_key_row_count = 0
    null_primary_key_sample_keys: tuple[str, ...] = ()
    duplicate_primary_key_sample_keys: tuple[str, ...] = ()

    if spec.primary_key and set(spec.primary_key).issubset(column_names):
        null_predicate = " OR ".join(
            f"{_quote_identifier(column_name)} IS NULL" for column_name in spec.primary_key
        )
        null_query = f"""
SELECT COUNT(*) AS null_primary_key_row_count
FROM {table_fqn}
WHERE {null_predicate}
""".strip()
        null_row = spark.sql(null_query).collect()[0]
        null_mapping = null_row.asDict(recursive=True) if hasattr(null_row, "asDict") else dict(null_row)
        null_primary_key_row_count = int(null_mapping["null_primary_key_row_count"] or 0)
        if null_primary_key_row_count > 0:
            null_primary_key_sample_keys = _collect_null_primary_key_samples(
                spark,
                table_fqn=table_fqn,
                primary_key=spec.primary_key,
                null_predicate=null_predicate,
            )

        group_by_clause = ", ".join(_quote_identifier(column_name) for column_name in spec.primary_key)
        duplicate_query = f"""
SELECT
    COUNT(*) AS duplicate_primary_key_group_count,
    COALESCE(SUM(group_count - 1), 0) AS duplicate_primary_key_row_count
FROM (
    SELECT COUNT(*) AS group_count
    FROM {table_fqn}
    GROUP BY {group_by_clause}
    HAVING COUNT(*) > 1
)
""".strip()
        duplicate_row = spark.sql(duplicate_query).collect()[0]
        duplicate_mapping = (
            duplicate_row.asDict(recursive=True)
            if hasattr(duplicate_row, "asDict")
            else dict(duplicate_row)
        )
        duplicate_primary_key_group_count = int(
            duplicate_mapping["duplicate_primary_key_group_count"] or 0
        )
        duplicate_primary_key_row_count = int(
            duplicate_mapping["duplicate_primary_key_row_count"] or 0
        )
        if duplicate_primary_key_group_count > 0:
            duplicate_primary_key_sample_keys = _collect_duplicate_primary_key_samples(
                spark,
                table_fqn=table_fqn,
                primary_key=spec.primary_key,
                group_by_clause=group_by_clause,
            )

    distinct_analysis_as_of_date_count = None
    if "analysis_as_of_date" in column_names:
        result_row = spark.sql(
            f"""
SELECT COUNT(DISTINCT CAST(analysis_as_of_date AS STRING)) AS distinct_analysis_as_of_date_count
FROM {table_fqn}
WHERE analysis_as_of_date IS NOT NULL
""".strip()
        ).collect()[0]
        mapping = result_row.asDict(recursive=True) if hasattr(result_row, "asDict") else dict(result_row)
        distinct_analysis_as_of_date_count = int(mapping["distinct_analysis_as_of_date_count"] or 0)

    distinct_scoring_scenario_count = None
    if "scoring_scenario" in column_names:
        result_row = spark.sql(
            f"""
SELECT COUNT(DISTINCT CAST(scoring_scenario AS STRING)) AS distinct_scoring_scenario_count
FROM {table_fqn}
WHERE scoring_scenario IS NOT NULL
""".strip()
        ).collect()[0]
        mapping = result_row.asDict(recursive=True) if hasattr(result_row, "asDict") else dict(result_row)
        distinct_scoring_scenario_count = int(mapping["distinct_scoring_scenario_count"] or 0)

    warning_count = 1 if row_count == 0 else 0
    anomaly_count = warning_count
    if null_primary_key_row_count > 0:
        anomaly_count += 1
    if duplicate_primary_key_group_count > 0:
        anomaly_count += 1
    status = "PASSED"
    if null_primary_key_row_count > 0 or duplicate_primary_key_group_count > 0:
        status = "FAILED"
    elif warning_count > 0:
        status = "WARN"

    return _GoldTableProfile(
        table_name=spec.table_name,
        table_fqn=table_fqn,
        build_order=spec.build_order,
        build_stage=spec.build_stage,
        primary_key=spec.primary_key,
        row_count=row_count,
        column_count=len(schema.fields),
        null_primary_key_row_count=null_primary_key_row_count,
        duplicate_primary_key_group_count=duplicate_primary_key_group_count,
        duplicate_primary_key_row_count=duplicate_primary_key_row_count,
        null_primary_key_sample_keys=null_primary_key_sample_keys,
        duplicate_primary_key_sample_keys=duplicate_primary_key_sample_keys,
        empty_table_flag=row_count == 0,
        distinct_analysis_as_of_date_count=distinct_analysis_as_of_date_count,
        distinct_scoring_scenario_count=distinct_scoring_scenario_count,
        anomaly_count=anomaly_count,
        warning_count=warning_count,
        status=status,
    )


def _collect_null_primary_key_samples(
    spark: Any,
    *,
    table_fqn: str,
    primary_key: tuple[str, ...],
    null_predicate: str,
    limit: int = 10,
) -> tuple[str, ...]:
    """Return bounded sample primary-key values for rows with null key fields."""
    sample_query = f"""
SELECT {_build_primary_key_sample_expression(primary_key)} AS sample_key
FROM {table_fqn}
WHERE {null_predicate}
LIMIT {limit}
""".strip()
    return _collect_sample_key_values(spark, sample_query)


def _collect_duplicate_primary_key_samples(
    spark: Any,
    *,
    table_fqn: str,
    primary_key: tuple[str, ...],
    group_by_clause: str,
    limit: int = 10,
) -> tuple[str, ...]:
    """Return bounded sample primary-key values for duplicate key groups."""
    sample_query = f"""
SELECT {_build_primary_key_sample_expression(primary_key)} AS sample_key
FROM (
    SELECT {group_by_clause}, COUNT(*) AS group_count
    FROM {table_fqn}
    GROUP BY {group_by_clause}
    HAVING COUNT(*) > 1
    ORDER BY group_count DESC
    LIMIT {limit}
)
""".strip()
    return _collect_sample_key_values(spark, sample_query)


def _collect_sample_key_values(spark: Any, sample_query: str) -> tuple[str, ...]:
    rows = spark.sql(sample_query).collect()
    sample_keys: list[str] = []
    for row in rows:
        mapping = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        sample_keys.append(str(mapping["sample_key"]))
    return tuple(sample_keys)


def _build_primary_key_sample_expression(primary_key: tuple[str, ...]) -> str:
    parts = [
        "CONCAT("
        f"'{column_name}=', "
        f"COALESCE(CAST({_quote_identifier(column_name)} AS STRING), '<NULL>')"
        ")"
        for column_name in primary_key
    ]
    return f"CONCAT_WS('|', {', '.join(parts)})"


def _build_table_profile_record(
    context: PipelineContext,
    profile: _GoldTableProfile,
    *,
    profiled_ts: datetime,
) -> dict[str, Any]:
    return {
        "pipeline_run_id": context.pipeline_run_id,
        "release_name": context.release_name,
        "analysis_as_of_date": context.analysis_as_of_date,
        "table_name": profile.table_name,
        "table_fqn": profile.table_fqn,
        "build_stage": profile.build_stage,
        "build_order": profile.build_order,
        "primary_key_columns": list(profile.primary_key),
        "row_count": profile.row_count,
        "column_count": profile.column_count,
        "null_primary_key_row_count": profile.null_primary_key_row_count,
        "duplicate_primary_key_group_count": profile.duplicate_primary_key_group_count,
        "duplicate_primary_key_row_count": profile.duplicate_primary_key_row_count,
        "empty_table_flag": profile.empty_table_flag,
        "distinct_analysis_as_of_date_count": profile.distinct_analysis_as_of_date_count,
        "distinct_scoring_scenario_count": profile.distinct_scoring_scenario_count,
        "anomaly_count": profile.anomaly_count,
        "warning_count": profile.warning_count,
        "status": profile.status,
        "profiled_ts": profiled_ts,
    }


def _build_column_profile_records(
    context: PipelineContext,
    table_name: str,
    column_profiles: dict[str, dict[str, Any]],
    *,
    profiled_ts: datetime,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for column_profile in column_profiles.values():
        records.append(
            {
                "pipeline_run_id": context.pipeline_run_id,
                "release_name": context.release_name,
                "analysis_as_of_date": context.analysis_as_of_date,
                "table_name": table_name,
                "column_name": column_profile["column_name"],
                "data_type": column_profile["data_type"],
                "nullable": column_profile["nullable"],
                "row_count": column_profile["row_count"],
                "non_null_count": column_profile["non_null_count"],
                "null_count": column_profile["null_count"],
                "null_pct": column_profile["null_pct"],
                "approx_distinct_count": column_profile["approx_distinct_count"],
                "min_value": column_profile["min_value"],
                "max_value": column_profile["max_value"],
                "profiled_ts": profiled_ts,
            }
        )
    return records


def _get_player_performance_window_count(config: SilverToGoldConfig) -> int:
    """Return the expected number of player performance evidence windows."""
    return len(
        {
            "career",
            f"trailing_{config.data['evidence_windows']['primary_window_days']}",
            f"trailing_{config.data['evidence_windows']['trend_window_days']}",
            f"trailing_{config.data['evidence_windows']['recent_window_days']}",
        }
    )


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _calculate_failure_pct(failed_count: int, evaluated_count: int) -> float | None:
    if evaluated_count <= 0:
        return 0.0
    return round((float(failed_count) / float(evaluated_count)) * 100.0, 4)
