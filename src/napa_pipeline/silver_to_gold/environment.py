"""Environment and runtime context helpers for the Silver-to-Gold pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from napa_pipeline.silver_to_gold.config import (
    SilverToGoldConfig,
    parse_optional_analysis_as_of_date,
)
from napa_pipeline.silver_to_gold.time_controls import resolve_analysis_as_of_date


class EnvironmentValidationError(RuntimeError):
    """Raised when the Databricks release environment is invalid."""


@dataclass(frozen=True)
class ManagedObjectStatus:
    """Status for a schema required by the pipeline."""

    object_type: str
    object_name: str
    existed: bool


@dataclass(frozen=True)
class ReleaseEnvironment:
    """Resolved Databricks object names for a Silver-to-Gold release."""

    catalog: str
    silver_schema: str
    gold_schema: str
    gold_stage_schema: str
    operations_schema: str


@dataclass(frozen=True)
class ReleaseEnvironmentStatus:
    """Outcome of validating or creating release-specific environment objects."""

    release_environment: ReleaseEnvironment
    schema_statuses: tuple[ManagedObjectStatus, ...]


@dataclass(frozen=True)
class GoldRuntimeContext:
    """Shared runtime context required by Gold tasks."""

    release_name: str
    release_role: str
    catalog: str
    silver_schema: str
    gold_schema: str
    stage_schema: str
    operations_schema: str
    analysis_as_of_date: date
    scoring_scenario: str
    model_enabled: bool
    authoritative_recommendation_flag: bool
    pipeline_version: str
    configuration_hash: str
    deterministic_seed: int
    upstream_silver_run_id: str


def resolve_release_environment(config: SilverToGoldConfig) -> ReleaseEnvironment:
    """Resolve release-specific schema names from config."""
    return ReleaseEnvironment(
        catalog=str(config.data["runtime"]["catalog"]),
        silver_schema=str(config.data["schemas"]["silver"]),
        gold_schema=str(config.data["schemas"]["gold"]),
        gold_stage_schema=str(config.data["schemas"]["gold_stage"]),
        operations_schema=str(config.data["schemas"]["operations"]),
    )


def ensure_release_environment(
    spark: Any,
    config: SilverToGoldConfig,
    create_missing: bool = True,
) -> ReleaseEnvironmentStatus:
    """Validate or create the schemas required for one Silver-to-Gold release."""
    environment = resolve_release_environment(config)
    existing_schemas = _get_existing_schemas(spark, environment.catalog)

    schema_statuses = []
    for schema_name in (
        environment.silver_schema,
        environment.gold_schema,
        environment.gold_stage_schema,
        environment.operations_schema,
    ):
        schema_existed = schema_name in existing_schemas
        if not schema_existed:
            if not create_missing:
                raise EnvironmentValidationError(
                    f"Required schema does not exist: {environment.catalog}.{schema_name}"
                )
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {environment.catalog}.{schema_name}")

        schema_statuses.append(
            ManagedObjectStatus(
                object_type="schema",
                object_name=f"{environment.catalog}.{schema_name}",
                existed=schema_existed,
            )
        )

    return ReleaseEnvironmentStatus(
        release_environment=environment,
        schema_statuses=tuple(schema_statuses),
    )


def build_runtime_context(
    config: SilverToGoldConfig,
    environment: ReleaseEnvironment,
    *,
    upstream_silver_run_id: str,
    match_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    analysis_as_of_date: str | date | None = None,
) -> GoldRuntimeContext:
    """Build a validated Gold runtime context from config and local fixtures."""
    if isinstance(analysis_as_of_date, date):
        resolved_analysis_as_of_date = analysis_as_of_date
    else:
        resolved_analysis_as_of_date = resolve_analysis_as_of_date(
            match_rows,
            explicit_analysis_as_of_date=parse_optional_analysis_as_of_date(analysis_as_of_date),
        )

    if not upstream_silver_run_id or not upstream_silver_run_id.strip():
        raise EnvironmentValidationError("upstream_silver_run_id must not be empty.")

    return GoldRuntimeContext(
        release_name=config.release_name,
        release_role=config.release_role,
        catalog=environment.catalog,
        silver_schema=environment.silver_schema,
        gold_schema=environment.gold_schema,
        stage_schema=environment.gold_stage_schema,
        operations_schema=environment.operations_schema,
        analysis_as_of_date=resolved_analysis_as_of_date,
        scoring_scenario=config.scoring_scenario,
        model_enabled=config.model_enabled,
        authoritative_recommendation_flag=bool(
            config.data["release"]["authoritative_recommendation_flag"]
        ),
        pipeline_version=str(config.data["project"]["pipeline_version"]),
        configuration_hash=config.config_hash,
        deterministic_seed=config.deterministic_seed,
        upstream_silver_run_id=upstream_silver_run_id.strip(),
    )


def _get_existing_schemas(spark: Any, catalog: str) -> set[str]:
    """Return existing schema names for the given catalog."""
    try:
        rows = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
    except Exception as exc:
        raise EnvironmentValidationError(
            f"Could not access catalog '{catalog}'."
        ) from exc

    schema_names = set()
    for row in rows:
        mapping = row.asDict() if hasattr(row, "asDict") else dict(row)
        for key in ("databaseName", "namespace", "schemaName"):
            value = mapping.get(key)
            if value:
                schema_names.add(str(value))
                break
    return schema_names
