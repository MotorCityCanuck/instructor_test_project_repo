"""Databricks entry point for selecting governed Olympic doubles team sets."""

from __future__ import annotations

import argparse
from uuid import uuid4

from _bootstrap_napa_pipeline import bootstrap_napa_pipeline_imports

bootstrap_napa_pipeline_imports()

from napa_pipeline.silver_to_gold.cli import (
    add_config_path_argument,
    add_release_name_argument,
    get_databricks_global,
    normalize_config_path,
    set_task_value,
)
from napa_pipeline.silver_to_gold.config import load_silver_to_gold_config
from napa_pipeline.silver_to_gold.environment import ensure_release_environment
from napa_pipeline.silver_to_gold.olympic_team_selection import (
    parse_team_set_count,
    publish_olympic_team_selections,
)


SCRIPT_VERSION = "2026.09.14.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select existing, eligible Olympic doubles teams from Gold scorecards."
    )
    add_release_name_argument(parser)
    add_config_path_argument(parser)
    parser.add_argument(
        "--team-set-count",
        required=True,
        help="Positive number of complete USA/Canada men's, women's, and mixed team sets.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    team_set_count = parse_team_set_count(args.team_set_count)
    spark = get_databricks_global("spark")
    dbutils = get_databricks_global("dbutils")
    config = load_silver_to_gold_config(
        args.release_name,
        config_root=normalize_config_path(args.config_path),
    )
    environment = ensure_release_environment(spark, config, create_missing=True).release_environment
    selection_run_id = str(uuid4())
    summary = publish_olympic_team_selections(
        spark,
        environment,
        dataset_source=config.release_name,
        team_set_count=team_set_count,
        selection_run_id=selection_run_id,
        scoring_scenario=config.scoring_scenario,
    )

    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Dataset source: {config.release_name}")
    print(f"Team set count: {team_set_count}")
    print(f"Selection run ID: {selection_run_id}")
    print(f"Gold source: {summary.source_table_fqn}")
    print(f"Gold output: {summary.target_table_fqn}")
    print(f"Selected rows: {summary.output_row_count}")
    spark.sql(
        f"""
SELECT
    selection_set_number,
    country_code,
    division,
    team_number,
    selection_score
FROM {summary.target_table_fqn}
ORDER BY selection_set_number, country_code, division
""".strip()
    ).show(truncate=False)
    set_task_value(dbutils, "selection_run_id", selection_run_id)
    set_task_value(dbutils, "release_name", config.release_name)
    set_task_value(dbutils, "team_set_count", team_set_count)
    set_task_value(dbutils, "olympic_team_selections_row_count", summary.output_row_count)


if __name__ == "__main__":
    main()
