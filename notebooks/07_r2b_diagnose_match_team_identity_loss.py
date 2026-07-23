"""Diagnose whether persistent team identity loss occurs before or during Raw-to-Bronze."""

from __future__ import annotations

import argparse
from typing import Any

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.raw_to_bronze.cli import (
    add_config_path_argument,
    add_release_type_argument,
    get_databricks_global,
    normalize_config_path,
    release_type_to_release_name,
    set_task_value,
)
from napa_pipeline.raw_to_bronze.bronze import get_bronze_target_table_fqn
from napa_pipeline.raw_to_bronze.config import load_raw_to_bronze_config
from napa_pipeline.raw_to_bronze.environment import resolve_release_environment
from napa_pipeline.raw_to_bronze.inventory import validate_raw_inventory_and_readiness


SCRIPT_VERSION = "2026.07.23.1"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the raw-versus-bronze diagnosis."""
    parser = argparse.ArgumentParser(
        description=(
            "Diagnose whether team identity loss for match_teams occurs in Raw input "
            "or during Raw-to-Bronze publication."
        )
    )
    add_release_type_argument(parser)
    add_config_path_argument(parser)
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="Maximum number of sample rows to print for null-team_id records.",
    )
    return parser.parse_args()


def main() -> None:
    """Compare raw parquet and Bronze tables for matches and match_teams identity coverage."""
    args = parse_args()
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")

    release_name = release_type_to_release_name(args.release_type)
    config = load_raw_to_bronze_config(
        release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment = resolve_release_environment(config)
    validation_result = validate_raw_inventory_and_readiness(
        spark,
        dbutils,
        config,
        environment,
    )
    source_readiness_by_name = {
        record.source_name: record for record in validation_result.source_readiness
    }

    raw_match_teams_path = source_readiness_by_name["match_teams"].file_path
    raw_matches_path = source_readiness_by_name["matches"].file_path
    bronze_match_teams_fqn = get_bronze_target_table_fqn(
        environment,
        {
            "bronze_table": config.data["sources"]["match_teams"]["bronze_table"],
        },
    )

    raw_match_teams_df = spark.read.parquet(raw_match_teams_path)
    raw_matches_df = spark.read.parquet(raw_matches_path)
    bronze_match_teams_df = spark.table(bronze_match_teams_fqn)

    raw_match_teams_df.createOrReplaceTempView("diag_raw_match_teams")
    raw_matches_df.createOrReplaceTempView("diag_raw_matches")
    bronze_match_teams_df.createOrReplaceTempView("diag_bronze_match_teams")

    raw_row_stats = _single_row(
        spark,
        """
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END) AS nonnull_team_id_rows,
    SUM(CASE WHEN team_number IS NOT NULL THEN 1 ELSE 0 END) AS nonnull_team_number_rows
FROM diag_raw_match_teams
""".strip(),
    )
    bronze_row_stats = _single_row(
        spark,
        """
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END) AS nonnull_team_id_rows,
    SUM(CASE WHEN team_number IS NOT NULL THEN 1 ELSE 0 END) AS nonnull_team_number_rows
FROM diag_bronze_match_teams
""".strip(),
    )
    raw_cardinality_distribution = _rows(
        spark,
        """
SELECT
    populated_team_id_count,
    COUNT(*) AS match_count
FROM (
    SELECT
        match_id,
        SUM(CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END) AS populated_team_id_count
    FROM diag_raw_match_teams
    GROUP BY match_id
) x
GROUP BY populated_team_id_count
ORDER BY populated_team_id_count
""".strip(),
    )
    bronze_cardinality_distribution = _rows(
        spark,
        """
SELECT
    populated_team_id_count,
    COUNT(*) AS match_count
FROM (
    SELECT
        match_id,
        SUM(CASE WHEN team_id IS NOT NULL THEN 1 ELSE 0 END) AS populated_team_id_count
    FROM diag_bronze_match_teams
    GROUP BY match_id
) x
GROUP BY populated_team_id_count
ORDER BY populated_team_id_count
""".strip(),
    )
    raw_team_count_distribution = _rows(
        spark,
        """
SELECT
    team_count,
    COUNT(*) AS match_count
FROM (
    SELECT
        match_id,
        COUNT(*) AS team_count
    FROM diag_raw_match_teams
    GROUP BY match_id
) x
GROUP BY team_count
ORDER BY team_count
""".strip(),
    )
    winner_join_stats = _single_row(
        spark,
        """
SELECT
    SUM(CASE WHEN team_id_join.team_number IS NOT NULL THEN 1 ELSE 0 END) AS join_on_team_id_rows,
    SUM(CASE WHEN match_team_id_join.team_number IS NOT NULL THEN 1 ELSE 0 END) AS join_on_match_team_row_id_rows
FROM diag_raw_matches m
LEFT JOIN diag_raw_match_teams team_id_join
  ON m.id = team_id_join.match_id
 AND m.winning_team_id = team_id_join.team_id
LEFT JOIN diag_raw_match_teams match_team_id_join
  ON m.id = match_team_id_join.match_id
 AND m.winning_team_id = match_team_id_join.id
""".strip(),
    )
    raw_null_samples = _rows(
        spark,
        f"""
SELECT
    id,
    match_id,
    team_number,
    team_id,
    team_score,
    average_team_rating
FROM diag_raw_match_teams
WHERE team_id IS NULL
ORDER BY match_id, team_number
LIMIT {int(args.sample_limit)}
""".strip(),
    )
    bronze_null_samples = _rows(
        spark,
        f"""
SELECT
    id,
    match_id,
    team_number,
    team_id,
    team_score,
    average_team_rating
FROM diag_bronze_match_teams
WHERE team_id IS NULL
ORDER BY match_id, team_number
LIMIT {int(args.sample_limit)}
""".strip(),
    )

    diagnosis = classify_identity_loss(raw_row_stats, bronze_row_stats)

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Release type: {args.release_type}")
    print(f"Release name: {release_name}")
    print(f"Raw volume path: {environment.raw_volume_path}")
    print(f"Raw match_teams path: {raw_match_teams_path}")
    print(f"Raw matches path: {raw_matches_path}")
    print(f"Bronze match_teams table: {bronze_match_teams_fqn}")
    print("")
    print("Raw row-level coverage:")
    _print_mapping(raw_row_stats)
    print("")
    print("Bronze row-level coverage:")
    _print_mapping(bronze_row_stats)
    print("")
    print("Raw match team-count distribution:")
    _print_rows(raw_team_count_distribution)
    print("")
    print("Raw populated team_id count per match:")
    _print_rows(raw_cardinality_distribution)
    print("")
    print("Bronze populated team_id count per match:")
    _print_rows(bronze_cardinality_distribution)
    print("")
    print("Raw winner join diagnostics:")
    _print_mapping(winner_join_stats)
    print("")
    print("Diagnosis:")
    print(diagnosis)
    print("")
    print("Sample raw null-team_id rows:")
    _print_rows(raw_null_samples)
    print("")
    print("Sample bronze null-team_id rows:")
    _print_rows(bronze_null_samples)

    set_task_value(dbutils, "release_name", release_name)
    set_task_value(dbutils, "raw_match_teams_path", raw_match_teams_path)
    set_task_value(dbutils, "bronze_match_teams_table", bronze_match_teams_fqn)
    set_task_value(dbutils, "raw_nonnull_team_id_rows", int(raw_row_stats["nonnull_team_id_rows"] or 0))
    set_task_value(dbutils, "bronze_nonnull_team_id_rows", int(bronze_row_stats["nonnull_team_id_rows"] or 0))
    set_task_value(dbutils, "winner_join_on_team_id_rows", int(winner_join_stats["join_on_team_id_rows"] or 0))
    set_task_value(
        dbutils,
        "winner_join_on_match_team_row_id_rows",
        int(winner_join_stats["join_on_match_team_row_id_rows"] or 0),
    )
    set_task_value(dbutils, "diagnosis", diagnosis)


def classify_identity_loss(
    raw_row_stats: dict[str, Any],
    bronze_row_stats: dict[str, Any],
) -> str:
    """Return a plain diagnosis based on raw-versus-bronze identity coverage."""
    raw_nonnull = int(raw_row_stats["nonnull_team_id_rows"] or 0)
    bronze_nonnull = int(bronze_row_stats["nonnull_team_id_rows"] or 0)

    if raw_nonnull == bronze_nonnull:
        return (
            "Raw and Bronze match_teams have identical non-null team_id coverage. "
            "The identity loss is upstream of Bronze publication, not caused by Raw-to-Bronze."
        )
    if raw_nonnull > bronze_nonnull:
        return (
            "Raw match_teams has more populated team_id values than Bronze. "
            "This indicates a Raw-to-Bronze mapping or publication defect."
        )
    return (
        "Bronze has more populated team_id values than Raw. "
        "This is unexpected and should trigger a manual review of the diagnostic assumptions."
    )


def _rows(spark: Any, query: str) -> list[dict[str, Any]]:
    return [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in spark.sql(query).collect()
    ]


def _single_row(spark: Any, query: str) -> dict[str, Any]:
    rows = _rows(spark, query)
    if not rows:
        raise RuntimeError(f"Expected one row from diagnostic query, received none: {query}")
    return rows[0]


def _print_mapping(mapping: dict[str, Any]) -> None:
    for key, value in mapping.items():
        print(f"- {key}: {value}")


def _print_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("<none>")
        return
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
