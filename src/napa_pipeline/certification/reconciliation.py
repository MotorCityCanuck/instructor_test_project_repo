"""Source reconciliation and regression helpers for Raw certification."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.models import CertificationRuleResult, InventoryCertificationResult


RELEASE_ORDER = {
    "napa_5k": 1,
    "napa_50k": 2,
    "napa_250k": 3,
}


@dataclass(frozen=True)
class ReleaseMetrics:
    """Current-release summary metrics used for reconciliation and regression."""

    file_count: int
    row_counts_by_source: dict[str, int]
    schema_hashes_by_source: dict[str, str]
    player_status_distribution: dict[str, int]
    team_count: int
    match_count: int
    average_rating: float | None
    rated_player_count: int
    active_player_rate: float | None
    candidate_team_count: int


def load_certification_snapshot(snapshot_path: str | Path | None) -> dict[str, Any] | None:
    """Load a certification or source snapshot from JSON when available."""
    if snapshot_path is None:
        return None
    path = Path(snapshot_path)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def build_release_metrics(
    spark: Any,
    inventory_result: InventoryCertificationResult,
) -> ReleaseMetrics:
    """Build current-release summary metrics for reconciliation and regression."""
    loaded_by_source = {
        source.source_name: source for source in inventory_result.loaded_sources
    }
    row_counts = {
        source.source_name: int(source.row_count or 0)
        for source in inventory_result.loaded_sources
        if source.row_count is not None
    }
    schema_hashes = {
        source.source_name: str(source.schema_hash or "")
        for source in inventory_result.loaded_sources
        if source.schema_hash is not None
    }

    player_status_distribution, average_rating, rated_player_count, active_player_rate = _build_player_metrics(
        spark,
        loaded_by_source.get("player_master"),
    )
    team_count = _table_row_count(loaded_by_source.get("teams"))
    match_count = _table_row_count(loaded_by_source.get("matches"))
    candidate_team_count = _build_candidate_team_count(
        spark,
        loaded_by_source.get("teams"),
        loaded_by_source.get("team_memberships"),
        loaded_by_source.get("match_teams"),
    )

    return ReleaseMetrics(
        file_count=len(
            [
                record
                for record in inventory_result.discovered_files
                if record.file_name.endswith(".parquet")
            ]
        ),
        row_counts_by_source=row_counts,
        schema_hashes_by_source=schema_hashes,
        player_status_distribution=player_status_distribution,
        team_count=team_count,
        match_count=match_count,
        average_rating=average_rating,
        rated_player_count=rated_player_count,
        active_player_rate=active_player_rate,
        candidate_team_count=candidate_team_count,
    )


def evaluate_reconciliation_and_regression_rules(
    config: CertificationConfig,
    metrics: ReleaseMetrics,
    *,
    source_snapshot: dict[str, Any] | None = None,
    prior_release_snapshot: dict[str, Any] | None = None,
    cross_scale_snapshots: list[dict[str, Any]] | None = None,
) -> tuple[CertificationRuleResult, ...]:
    """Evaluate source reconciliation and prior-release regression rules."""
    thresholds = config.profile_thresholds
    results: list[CertificationRuleResult] = []
    results.extend(
        _evaluate_source_snapshot_reconciliation(
            config.release_name,
            metrics,
            thresholds,
            source_snapshot,
        )
    )
    results.extend(
        _evaluate_cross_scale_consistency(
            config.release_name,
            metrics,
            cross_scale_snapshots or [],
        )
    )
    results.extend(
        _evaluate_prior_release_regression(
            config.release_name,
            metrics,
            thresholds,
            prior_release_snapshot,
        )
    )
    return tuple(results)


def _evaluate_source_snapshot_reconciliation(
    release_name: str,
    metrics: ReleaseMetrics,
    thresholds: dict[str, Any],
    source_snapshot: dict[str, Any] | None,
) -> list[CertificationRuleResult]:
    if source_snapshot is None:
        return [
            _warning_rule(
                "RAW_SOURCE_FILE_COUNT_RECONCILIATION",
                "Source file-count reconciliation evidence is available",
                "Source Reconciliation and Regression",
                "reconciliation",
                "No source/export snapshot was supplied; source reconciliation was skipped.",
            ),
            _warning_rule(
                "RAW_SOURCE_PLAYER_STATUS_RECONCILIATION",
                "Player-status reconciliation evidence is available",
                "Source Reconciliation and Regression",
                "reconciliation",
                "No source/export snapshot was supplied; player-status reconciliation was skipped.",
            ),
        ]

    rules: list[CertificationRuleResult] = []
    source_row_counts = source_snapshot.get("file_row_counts") or {}
    source_schema_hashes = source_snapshot.get("schema_hashes") or {}
    source_player_status = source_snapshot.get("player_status_distribution") or {}
    source_team_count = int(source_snapshot.get("team_count") or 0)
    source_match_count = int(source_snapshot.get("match_count") or 0)
    source_average_rating = source_snapshot.get("average_rating")

    row_tolerance = float(thresholds["maximum_source_row_count_relative_diff"])
    file_mismatch_count = 0
    for source_name, current_count in metrics.row_counts_by_source.items():
        file_name = _source_name_to_file_name(source_name)
        expected_count = source_row_counts.get(file_name, source_row_counts.get(source_name))
        if expected_count is None:
            continue
        if _relative_diff(float(current_count), float(expected_count)) > row_tolerance:
            file_mismatch_count += 1

    rules.append(
        CertificationRuleResult(
            rule_id="RAW_SOURCE_FILE_COUNT_RECONCILIATION",
            name="Raw file counts match source/export evidence",
            pillar="Source Reconciliation and Regression",
            category="reconciliation",
            status="PASS" if file_mismatch_count == 0 else "FAIL",
            severity="info" if file_mismatch_count == 0 else "error",
            message=(
                "Raw file row counts reconcile to source/export evidence."
                if file_mismatch_count == 0
                else f"{file_mismatch_count} Raw source domains failed row-count reconciliation."
            ),
            affected_count=file_mismatch_count,
            expected_value=row_tolerance,
        )
    )

    status_tolerance = float(thresholds["maximum_status_distribution_pct_diff"])
    status_failures = 0
    source_total = sum(int(value or 0) for value in source_player_status.values())
    current_total = sum(int(value or 0) for value in metrics.player_status_distribution.values())
    if source_total > 0 and current_total > 0:
        all_statuses = set(source_player_status) | set(metrics.player_status_distribution)
        for status_name in all_statuses:
            source_rate = (int(source_player_status.get(status_name, 0)) / source_total)
            current_rate = (int(metrics.player_status_distribution.get(status_name, 0)) / current_total)
            if abs(current_rate - source_rate) > status_tolerance:
                status_failures += 1
    rules.append(
        CertificationRuleResult(
            rule_id="RAW_SOURCE_PLAYER_STATUS_RECONCILIATION",
            name="Player-status distribution matches source/export evidence",
            pillar="Source Reconciliation and Regression",
            category="reconciliation",
            status="PASS" if status_failures == 0 else "FAIL",
            severity="info" if status_failures == 0 else "blocker",
            message=(
                "Player-status distribution reconciles to source/export evidence."
                if status_failures == 0
                else f"{status_failures} player-status buckets drifted beyond the allowed tolerance."
            ),
            affected_count=status_failures,
            expected_value=status_tolerance,
        )
    )

    rules.append(
        _count_reconciliation_rule(
            "RAW_SOURCE_TEAM_COUNT_RECONCILIATION",
            "Team population was preserved",
            metrics.team_count,
            source_team_count,
            row_tolerance,
        )
    )
    rules.append(
        _count_reconciliation_rule(
            "RAW_SOURCE_MATCH_COUNT_RECONCILIATION",
            "Match population was preserved",
            metrics.match_count,
            source_match_count,
            row_tolerance,
        )
    )

    if source_average_rating is None or metrics.average_rating is None:
        rules.append(
            _warning_rule(
                "RAW_SOURCE_RATING_RECONCILIATION",
                "Rating-distribution reconciliation evidence is available",
                "Source Reconciliation and Regression",
                "reconciliation",
                "Average rating could not be reconciled because source or current rating evidence is missing.",
            )
        )
    else:
        rating_tolerance = float(thresholds["maximum_rating_average_diff"])
        diff = abs(float(metrics.average_rating) - float(source_average_rating))
        rules.append(
            CertificationRuleResult(
                rule_id="RAW_SOURCE_RATING_RECONCILIATION",
                name="Average rating was preserved through export",
                pillar="Source Reconciliation and Regression",
                category="reconciliation",
                status="PASS" if diff <= rating_tolerance else "FAIL",
                severity="info" if diff <= rating_tolerance else "error",
                message=(
                    "Average rating reconciles to source/export evidence."
                    if diff <= rating_tolerance
                    else f"Average rating drifted by {diff:.4f}, above tolerance {rating_tolerance:.4f}."
                ),
                affected_count=0 if diff <= rating_tolerance else 1,
                observed_value=round(diff, 6),
                expected_value=rating_tolerance,
            )
        )

    return rules


def _evaluate_cross_scale_consistency(
    release_name: str,
    metrics: ReleaseMetrics,
    cross_scale_snapshots: list[dict[str, Any]],
) -> list[CertificationRuleResult]:
    schema_mismatches = 0
    for snapshot in cross_scale_snapshots:
        snapshot_release = str(snapshot.get("release_name") or "")
        if not snapshot_release or snapshot_release == release_name:
            continue
        snapshot_hashes = snapshot.get("schema_hashes") or {}
        for source_name, current_hash in metrics.schema_hashes_by_source.items():
            comparison_hash = snapshot_hashes.get(_source_name_to_file_name(source_name), snapshot_hashes.get(source_name))
            if comparison_hash is None:
                continue
            if str(comparison_hash) != current_hash:
                schema_mismatches += 1
    if not cross_scale_snapshots:
        return [
            _warning_rule(
                "RAW_SCHEMA_CROSS_SCALE_CONSISTENCY",
                "Cross-scale schema comparison evidence is available",
                "Source Reconciliation and Regression",
                "regression",
                "No cross-scale certification snapshots were supplied; schema consistency comparison was skipped.",
            )
        ]
    return [
        CertificationRuleResult(
            rule_id="RAW_SCHEMA_CROSS_SCALE_CONSISTENCY",
            name="5K, 50K, and 250K use the same supported schema",
            pillar="Source Reconciliation and Regression",
            category="regression",
            status="PASS" if schema_mismatches == 0 else "FAIL",
            severity="info" if schema_mismatches == 0 else "blocker",
            message=(
                "Schema hashes are consistent across supplied scale snapshots."
                if schema_mismatches == 0
                else f"{schema_mismatches} schema-hash mismatches were found across supplied scale snapshots."
            ),
            affected_count=schema_mismatches,
        )
    ]


def _evaluate_prior_release_regression(
    release_name: str,
    metrics: ReleaseMetrics,
    thresholds: dict[str, Any],
    prior_release_snapshot: dict[str, Any] | None,
) -> list[CertificationRuleResult]:
    if prior_release_snapshot is None:
        return [
            _warning_rule(
                "RAW_DISTRIBUTION_PRIOR_RELEASE_DRIFT",
                "Prior-release comparison evidence is available",
                "Source Reconciliation and Regression",
                "regression",
                "No prior approved certification snapshot was supplied; prior-release drift checks were skipped.",
            )
        ]

    results: list[CertificationRuleResult] = []
    current_order = RELEASE_ORDER.get(release_name, 0)
    prior_release_name = str(prior_release_snapshot.get("release_name") or "")
    prior_order = RELEASE_ORDER.get(prior_release_name, 0)
    if prior_order and current_order and prior_order > current_order:
        results.append(
            _warning_rule(
                "RAW_DISTRIBUTION_PRIOR_RELEASE_DRIFT",
                "Prior-release comparison uses a lower or equal scale baseline",
                "Source Reconciliation and Regression",
                "regression",
                f"Prior snapshot release '{prior_release_name}' is not lower/equal scale than '{release_name}'.",
            )
        )
        return results

    prior_active_rate = prior_release_snapshot.get("active_player_rate")
    if prior_active_rate is None or metrics.active_player_rate is None:
        results.append(
            _warning_rule(
                "RAW_DISTRIBUTION_PRIOR_RELEASE_DRIFT",
                "Prior-release active-player drift can be evaluated",
                "Source Reconciliation and Regression",
                "regression",
                "Active-player drift could not be evaluated because one side is missing active-rate evidence.",
            )
        )
    else:
        drift_tolerance = float(thresholds["maximum_prior_release_active_rate_drift"])
        drift = abs(float(metrics.active_player_rate) - float(prior_active_rate))
        results.append(
            CertificationRuleResult(
                rule_id="RAW_DISTRIBUTION_PRIOR_RELEASE_DRIFT",
                name="Material prior-release active-player drift is identified",
                pillar="Source Reconciliation and Regression",
                category="regression",
                status="PASS" if drift <= drift_tolerance else "FAIL",
                severity="info" if drift <= drift_tolerance else "warning",
                message=(
                    "Prior-release active-player drift is within tolerance."
                    if drift <= drift_tolerance
                    else f"Prior-release active-player drift is {drift:.4f}, above tolerance {drift_tolerance:.4f}."
                ),
                affected_count=0 if drift <= drift_tolerance else 1,
                observed_value=round(drift, 6),
                expected_value=drift_tolerance,
            )
        )

    if release_name in {"napa_50k", "napa_250k"}:
        prior_candidate_team_count = int(prior_release_snapshot.get("candidate_team_count") or 0)
        ratio_threshold = float(thresholds["minimum_candidate_pool_regression_ratio"])
        if prior_candidate_team_count <= 0:
            results.append(
                _warning_rule(
                    "RAW_CANDIDATE_POOL_PRIOR_RELEASE_REGRESSION",
                    "Prior-release candidate pool regression can be evaluated",
                    "Source Reconciliation and Regression",
                    "regression",
                    "Prior snapshot did not contain candidate-team evidence; regression comparison was skipped.",
                )
            )
        else:
            ratio = float(metrics.candidate_team_count) / float(prior_candidate_team_count)
            results.append(
                CertificationRuleResult(
                    rule_id="RAW_CANDIDATE_POOL_PRIOR_RELEASE_REGRESSION",
                    name="Candidate populations do not collapse unexpectedly",
                    pillar="Source Reconciliation and Regression",
                    category="regression",
                    status="PASS" if ratio >= ratio_threshold else "FAIL",
                    severity="info" if ratio >= ratio_threshold else "error",
                    message=(
                        "Candidate pool remains within the acceptable prior-release ratio."
                        if ratio >= ratio_threshold
                        else f"Candidate pool regression ratio is {ratio:.4f}, below minimum {ratio_threshold:.4f}."
                    ),
                    affected_count=0 if ratio >= ratio_threshold else 1,
                    observed_value=round(ratio, 6),
                    expected_value=ratio_threshold,
                )
            )
    return results


def _build_player_metrics(
    spark: Any,
    player_source: Any | None,
) -> tuple[dict[str, int], float | None, int, float | None]:
    if player_source is None or player_source.read_status == "UNREADABLE":
        return {}, None, 0, None
    columns = {field["column_name"] for field in player_source.schema_fields}
    rating_column = _resolve_first_column(columns, ("rating", "player_rating"))
    active_expression = _resolve_active_player_expression(columns)
    status_expression = _resolve_status_expression(columns)
    if rating_column is None or status_expression is None:
        return {}, None, 0, None
    query = f"""
SELECT
    {status_expression} AS player_status_group,
    COUNT(*) AS player_count,
    AVG(CASE WHEN {rating_column} IS NOT NULL THEN CAST({rating_column} AS DOUBLE) END) AS average_rating
FROM {player_source.temp_view_name}
GROUP BY {status_expression}
""".strip()
    rows = spark.sql(f"/* PLAYER_METRICS */\n{query}").collect()
    status_distribution: dict[str, int] = {}
    average_ratings: list[float] = []
    total_players = 0
    active_players = 0
    for row in rows:
        mapping = row.asDict() if hasattr(row, "asDict") else dict(row)
        status_name = str(mapping.get("player_status_group") or "UNKNOWN")
        player_count = int(mapping.get("player_count") or 0)
        status_distribution[status_name] = player_count
        total_players += player_count
        if status_name == "ACTIVE":
            active_players += player_count
        avg_value = mapping.get("average_rating")
        if avg_value is not None:
            average_ratings.append(float(avg_value))

    rated_players_query = f"""
SELECT COUNT(*) AS value
FROM {player_source.temp_view_name}
WHERE {rating_column} IS NOT NULL
""".strip()
    rated_players = int(_run_single_value_query(spark, "PLAYER_RATED_COUNT", rated_players_query))
    average_rating = None if not average_ratings else sum(average_ratings) / len(average_ratings)
    active_rate = None if total_players == 0 else (active_players / total_players)
    return status_distribution, average_rating, rated_players, active_rate


def _build_candidate_team_count(
    spark: Any,
    team_source: Any | None,
    memberships: Any | None,
    match_teams: Any | None,
) -> int:
    if (
        team_source is None
        or memberships is None
        or match_teams is None
        or team_source.read_status == "UNREADABLE"
        or memberships.read_status == "UNREADABLE"
        or match_teams.read_status == "UNREADABLE"
    ):
        return 0
    query = f"""
WITH team_member_counts AS (
    SELECT team_id, COUNT(DISTINCT player_id) AS member_count
    FROM {memberships.temp_view_name}
    GROUP BY team_id
),
team_match_counts AS (
    SELECT CAST(team_id AS STRING) AS team_id, COUNT(*) AS match_count
    FROM {match_teams.temp_view_name}
    WHERE team_id IS NOT NULL
    GROUP BY CAST(team_id AS STRING)
)
SELECT COUNT(*) AS value
FROM {team_source.temp_view_name} AS t
INNER JOIN team_member_counts AS m
    ON CAST(t.id AS STRING) = CAST(m.team_id AS STRING)
INNER JOIN team_match_counts AS mt
    ON CAST(t.id AS STRING) = mt.team_id
WHERE UPPER(TRIM(CAST(t.team_status AS STRING))) = 'ACTIVE'
  AND m.member_count = 2
  AND mt.match_count >= 1
""".strip()
    return int(_run_single_value_query(spark, "CANDIDATE_TEAM_COUNT", query))


def _count_reconciliation_rule(
    rule_id: str,
    name: str,
    current_count: int,
    expected_count: int,
    tolerance: float,
) -> CertificationRuleResult:
    diff = _relative_diff(float(current_count), float(expected_count))
    return CertificationRuleResult(
        rule_id=rule_id,
        name=name,
        pillar="Source Reconciliation and Regression",
        category="reconciliation",
        status="PASS" if diff <= tolerance else "FAIL",
        severity="info" if diff <= tolerance else "error",
        message=(
            f"{name}."
            if diff <= tolerance
            else (
                f"{name} failed: current={current_count}, expected={expected_count}, "
                f"relative_diff={diff:.4f}, tolerance={tolerance:.4f}."
            )
        ),
        affected_count=0 if diff <= tolerance else 1,
        observed_value=round(diff, 6),
        expected_value=tolerance,
    )


def _resolve_active_player_expression(columns: set[str]) -> str | None:
    if "active_flag" in columns:
        return "active_flag = true"
    status_column = _resolve_first_column(columns, ("player_status", "status"))
    if status_column is not None:
        return f"UPPER(TRIM(CAST({status_column} AS STRING))) = 'ACTIVE'"
    return None


def _resolve_status_expression(columns: set[str]) -> str | None:
    if "active_flag" in columns:
        return "CASE WHEN active_flag = true THEN 'ACTIVE' ELSE 'INACTIVE' END"
    status_column = _resolve_first_column(columns, ("player_status", "status"))
    if status_column is not None:
        return f"COALESCE(UPPER(TRIM(CAST({status_column} AS STRING))), 'UNKNOWN')"
    return None


def _resolve_first_column(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for column_name in candidates:
        if column_name in columns:
            return column_name
    return None


def _run_single_value_query(spark: Any, query_tag: str, sql_text: str) -> int:
    row = spark.sql(f"/* {query_tag} */\n{sql_text}").collect()[0]
    mapping = row.asDict() if hasattr(row, "asDict") else dict(row)
    return int(mapping.get("value", 0) or 0)


def _relative_diff(current_value: float, expected_value: float) -> float:
    denominator = max(abs(expected_value), 1.0)
    return abs(current_value - expected_value) / denominator


def _source_name_to_file_name(source_name: str) -> str:
    return f"{source_name}.parquet"


def _table_row_count(source: Any | None) -> int:
    if source is None or source.row_count is None:
        return 0
    return int(source.row_count)


def _warning_rule(
    rule_id: str,
    name: str,
    pillar: str,
    category: str,
    message: str,
) -> CertificationRuleResult:
    return CertificationRuleResult(
        rule_id=rule_id,
        name=name,
        pillar=pillar,
        category=category,
        status="WARN",
        severity="warning",
        message=message,
    )
