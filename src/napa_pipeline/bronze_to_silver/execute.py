"""Execution helpers for Bronze-to-Silver Databricks workflow tasks."""

from __future__ import annotations

from typing import Any

from napa_pipeline.bronze_to_silver.athlete import (
    build_player_assessment_history,
    build_player_registrations,
    build_players,
)
from napa_pipeline.bronze_to_silver.competition import (
    build_match_games,
    build_match_team_players,
    build_match_teams,
    build_matches,
)
from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig
from napa_pipeline.bronze_to_silver.cross_table import run_cross_table_validations
from napa_pipeline.bronze_to_silver.environment import ReleaseEnvironment
from napa_pipeline.bronze_to_silver.io import (
    get_bronze_source_table_fqn,
    get_silver_reject_table_fqn,
    get_silver_target_table_fqn,
)
from napa_pipeline.bronze_to_silver.operations import (
    PIPELINE_RUNS_TABLE,
    RECONCILIATION_RESULTS_TABLE,
    RUN_MESSAGES_TABLE,
    TABLE_RUNS_TABLE,
    PipelineContext,
    append_records,
    build_pipeline_run_start_record,
    build_reconciliation_record,
    build_run_message_record,
    build_table_run_end_record,
    build_table_run_start_record,
    ensure_operations_tables,
    utc_now,
)
from napa_pipeline.bronze_to_silver.orchestration import get_silver_tables_by_stage
from napa_pipeline.bronze_to_silver.organization import (
    build_club_memberships,
    build_clubs,
    build_team_memberships,
    build_teams,
)
from napa_pipeline.bronze_to_silver.publish import (
    append_quality_results_for_rejects,
    append_schema_snapshot_for_table,
    append_warning_message,
    collect_table_rows,
    publish_records_to_table,
    publish_records_to_view,
)
from napa_pipeline.bronze_to_silver.reference import (
    SilverBuildResult,
    build_monthly_batches,
    build_regions,
)
from napa_pipeline.bronze_to_silver.views import (
    build_vw_current_team_memberships,
    build_vw_match_results,
    build_vw_player_match_history,
    build_vw_players_current,
    build_vw_team_rosters,
)


TRANSFORM_BUILDERS = {
    "build_monthly_batches": build_monthly_batches,
    "build_regions": build_regions,
    "build_players": build_players,
    "build_player_registrations": build_player_registrations,
    "build_player_assessment_history": build_player_assessment_history,
    "build_clubs": build_clubs,
    "build_club_memberships": build_club_memberships,
    "build_teams": build_teams,
    "build_team_memberships": build_team_memberships,
    "build_matches": build_matches,
    "build_match_teams": build_match_teams,
    "build_match_team_players": build_match_team_players,
    "build_match_games": build_match_games,
}


def initialize_pipeline_run(
    spark: Any,
    context: PipelineContext,
) -> None:
    """Ensure operations tables exist and append the pipeline start record once."""
    ensure_operations_tables(spark, context)
    existing_rows = [
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        for row in spark.table(f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}").toLocalIterator()
        if (row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)).get("pipeline_run_id") == context.pipeline_run_id
    ]
    if existing_rows:
        return
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{PIPELINE_RUNS_TABLE}",
        [build_pipeline_run_start_record(context, status="VALIDATING")],
    )


def execute_stage(
    spark: Any,
    config: BronzeToSilverConfig,
    environment: ReleaseEnvironment,
    context: PipelineContext,
    *,
    stage_name: str,
) -> list[dict[str, Any]]:
    """Execute and publish all configured Silver tables in one stage."""
    initialize_pipeline_run(spark, context)
    stage_tables = get_silver_tables_by_stage(config).get(stage_name, [])
    completed_records: list[dict[str, Any]] = []

    for table_config in stage_tables:
        source_name = str(table_config["source"])
        target_table = str(table_config["target"])
        source_table = str(config.enabled_sources[source_name]["bronze_table"])
        started_ts = utc_now()
        append_records(
            spark,
            f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
            [
                build_table_run_start_record(
                    context,
                    source_table=source_table,
                    target_table=target_table,
                    build_stage=stage_name,
                    build_order=int(table_config["build_order"]),
                    started_ts=started_ts,
                )
            ],
        )

        try:
            source_rows = collect_table_rows(
                spark,
                get_bronze_source_table_fqn(environment, config.enabled_sources[source_name]),
            )
            build_result = _execute_single_table(
                spark,
                config,
                environment,
                context,
                table_config=table_config,
                source_rows=source_rows,
            )
            end_record = build_table_run_end_record(
                context,
                source_table=source_table,
                target_table=target_table,
                build_stage=stage_name,
                build_order=int(table_config["build_order"]),
                started_ts=started_ts,
                status="SUCCEEDED",
                source_row_count=len(source_rows),
                exact_duplicate_count=build_result.exact_duplicate_count,
                business_key_duplicate_count=build_result.business_key_duplicate_count,
                accepted_row_count=len(build_result.accepted_rows),
                rejected_row_count=len(build_result.rejected_rows),
                warning_count=build_result.warning_count,
                published_row_count=len(build_result.accepted_rows),
            )
        except Exception as exc:
            end_record = build_table_run_end_record(
                context,
                source_table=source_table,
                target_table=target_table,
                build_stage=stage_name,
                build_order=int(table_config["build_order"]),
                started_ts=started_ts,
                status="FAILED",
                error_message=str(exc),
            )
            append_records(
                spark,
                f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
                [end_record],
            )
            raise

        append_records(
            spark,
            f"{context.operations_schema_fqn}.{TABLE_RUNS_TABLE}",
            [end_record],
        )
        completed_records.append(end_record)

    return completed_records


def run_cross_table_validation_task(
    spark: Any,
    config: BronzeToSilverConfig,
    environment: ReleaseEnvironment,
    context: PipelineContext,
) -> dict[str, int]:
    """Execute cross-table validation against published Silver tables."""
    initialize_pipeline_run(spark, context)
    table_rows = _load_published_silver_rows(spark, environment)
    result = run_cross_table_validations(
        context,
        players_rows=table_rows["players"],
        teams_rows=table_rows["teams"],
        team_memberships_rows=table_rows["team_memberships"],
        matches_rows=table_rows["matches"],
        match_teams_rows=table_rows["match_teams"],
        match_team_players_rows=table_rows["match_team_players"],
        match_games_rows=table_rows["match_games"],
        expected_match_team_count=int(config.data["thresholds"]["expected_match_team_count"]),
        expected_match_team_player_count=int(config.data["thresholds"]["expected_match_team_player_count"]),
    )
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RUN_MESSAGES_TABLE}",
        list(result.run_messages),
    )
    append_records(
        spark,
        f"{context.operations_schema_fqn}.quality_results",
        list(result.quality_results),
    )
    return {
        "quality_result_count": len(result.quality_results),
        "warning_count": result.warning_count,
        "failure_count": result.failure_count,
    }


def publish_convenience_views_task(
    spark: Any,
    config: BronzeToSilverConfig,
    environment: ReleaseEnvironment,
    context: PipelineContext,
) -> dict[str, int]:
    """Publish the configured convenience views."""
    table_rows = _load_published_silver_rows(spark, environment)
    view_prefix = f"{environment.catalog}.{environment.silver_schema}"
    published_counts = {
        "vw_players_current": publish_records_to_view(
            spark,
            f"{view_prefix}.vw_players_current",
            build_vw_players_current(table_rows["players"]),
        ),
        "vw_current_team_memberships": publish_records_to_view(
            spark,
            f"{view_prefix}.vw_current_team_memberships",
            build_vw_current_team_memberships(table_rows["team_memberships"]),
        ),
        "vw_team_rosters": publish_records_to_view(
            spark,
            f"{view_prefix}.vw_team_rosters",
            build_vw_team_rosters(
                table_rows["teams"],
                table_rows["players"],
                table_rows["team_memberships"],
                expected_roster_count=int(config.data["thresholds"]["expected_match_team_player_count"]),
            ),
        ),
        "vw_match_results": publish_records_to_view(
            spark,
            f"{view_prefix}.vw_match_results",
            build_vw_match_results(
                table_rows["matches"],
                table_rows["match_teams"],
                table_rows["match_games"],
                table_rows["teams"],
                table_rows["regions"],
                table_rows["monthly_batches"],
            ),
        ),
        "vw_player_match_history": publish_records_to_view(
            spark,
            f"{view_prefix}.vw_player_match_history",
            build_vw_player_match_history(
                table_rows["match_team_players"],
                table_rows["match_teams"],
                table_rows["matches"],
                table_rows["players"],
                table_rows["teams"],
                table_rows["regions"],
                table_rows["monthly_batches"],
            ),
        ),
    }
    return published_counts


def _execute_single_table(
    spark: Any,
    config: BronzeToSilverConfig,
    environment: ReleaseEnvironment,
    context: PipelineContext,
    *,
    table_config: dict[str, Any],
    source_rows: list[dict[str, Any]],
) -> SilverBuildResult:
    builder = TRANSFORM_BUILDERS[str(table_config["transform"])]
    target_table = str(table_config["target"])
    kwargs = _resolve_builder_kwargs(
        spark,
        environment,
        target_table=target_table,
    )
    build_result = builder(source_rows, config, context, **kwargs)

    target_fqn = get_silver_target_table_fqn(environment, table_config)
    reject_fqn = get_silver_reject_table_fqn(environment, table_config)
    publish_records_to_table(spark, target_fqn, build_result.accepted_rows)
    publish_records_to_table(spark, reject_fqn, build_result.rejected_rows)
    append_records(
        spark,
        f"{context.operations_schema_fqn}.{RECONCILIATION_RESULTS_TABLE}",
        [
            build_reconciliation_record(
                context,
                source_table=str(config.enabled_sources[str(table_config["source"])]["bronze_table"]),
                target_table=target_table,
                bronze_row_count=build_result.reconciliation.bronze_row_count,
                exact_duplicate_count=build_result.reconciliation.exact_duplicate_count,
                business_key_loser_count=build_result.reconciliation.business_key_loser_count,
                rejected_row_count=build_result.reconciliation.rejected_row_count,
                accepted_row_count=build_result.reconciliation.accepted_row_count,
                status=build_result.reconciliation.status,
            )
        ],
    )
    append_quality_results_for_rejects(
        spark,
        context,
        target_table=target_table,
        evaluated_row_count=build_result.reconciliation.bronze_row_count,
        rejected_rows=build_result.rejected_rows,
    )
    append_schema_snapshot_for_table(
        spark,
        context,
        layer_name="silver",
        table_name=target_table,
        table_fqn=target_fqn,
    )
    if build_result.warning_count:
        append_warning_message(
            spark,
            context,
            target_table=target_table,
            message_code="TABLE_WARNING_COUNT",
            message_text=f"{target_table} completed with {build_result.warning_count} warning findings.",
        )
    return build_result


def _resolve_builder_kwargs(
    spark: Any,
    environment: ReleaseEnvironment,
    *,
    target_table: str,
) -> dict[str, Any]:
    base_tables = {
        "monthly_batches": _maybe_collect_silver_table(spark, environment, "monthly_batches"),
        "regions": _maybe_collect_silver_table(spark, environment, "regions"),
        "players": _maybe_collect_silver_table(spark, environment, "players"),
        "clubs": _maybe_collect_silver_table(spark, environment, "clubs"),
        "teams": _maybe_collect_silver_table(spark, environment, "teams"),
        "team_memberships": _maybe_collect_silver_table(spark, environment, "team_memberships"),
        "matches": _maybe_collect_silver_table(spark, environment, "matches"),
        "match_teams": _maybe_collect_silver_table(spark, environment, "match_teams"),
    }
    mapping = {
        "monthly_batches": {},
        "regions": {},
        "players": {
            "regions_rows": base_tables["regions"],
            "monthly_batches_rows": base_tables["monthly_batches"],
        },
        "clubs": {"regions_rows": base_tables["regions"]},
        "teams": {"monthly_batches_rows": base_tables["monthly_batches"]},
        "player_registrations": {
            "players_rows": base_tables["players"],
            "monthly_batches_rows": base_tables["monthly_batches"],
        },
        "player_assessment_history": {
            "players_rows": base_tables["players"],
            "monthly_batches_rows": base_tables["monthly_batches"],
        },
        "club_memberships": {
            "players_rows": base_tables["players"],
            "clubs_rows": base_tables["clubs"],
            "monthly_batches_rows": base_tables["monthly_batches"],
        },
        "team_memberships": {
            "players_rows": base_tables["players"],
            "teams_rows": base_tables["teams"],
            "monthly_batches_rows": base_tables["monthly_batches"],
        },
        "matches": {
            "monthly_batches_rows": base_tables["monthly_batches"],
            "regions_rows": base_tables["regions"],
        },
        "match_teams": {
            "matches_rows": base_tables["matches"],
            "teams_rows": base_tables["teams"],
        },
        "match_team_players": {
            "match_teams_rows": base_tables["match_teams"],
            "players_rows": base_tables["players"],
            "team_memberships_rows": base_tables["team_memberships"],
        },
        "match_games": {
            "matches_rows": base_tables["matches"],
        },
    }
    return mapping[target_table]


def _maybe_collect_silver_table(
    spark: Any,
    environment: ReleaseEnvironment,
    table_name: str,
) -> list[dict[str, Any]]:
    table_fqn = f"{environment.catalog}.{environment.silver_schema}.{table_name}"
    if not spark.catalog.tableExists(table_fqn):
        return []
    return collect_table_rows(spark, table_fqn)


def _load_published_silver_rows(
    spark: Any,
    environment: ReleaseEnvironment,
) -> dict[str, list[dict[str, Any]]]:
    return {
        table_name: _maybe_collect_silver_table(spark, environment, table_name)
        for table_name in (
            "monthly_batches",
            "regions",
            "players",
            "teams",
            "team_memberships",
            "matches",
            "match_teams",
            "match_team_players",
            "match_games",
        )
    }
