"""Environment setup and validation helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from napa_pipeline.bronze_to_silver.config import BronzeToSilverConfig


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
    """Resolved Databricks object names for a Bronze-to-Silver release."""

    catalog: str
    bronze_schema: str
    silver_schema: str
    silver_reject_schema: str
    operations_schema: str


@dataclass(frozen=True)
class ReleaseEnvironmentStatus:
    """Outcome of validating or creating release-specific environment objects."""

    release_environment: ReleaseEnvironment
    schema_statuses: tuple[ManagedObjectStatus, ...]


def resolve_release_environment(config: BronzeToSilverConfig) -> ReleaseEnvironment:
    """Resolve release-specific schema names from config."""
    return ReleaseEnvironment(
        catalog=str(config.data["runtime"]["catalog"]),
        bronze_schema=str(config.data["schemas"]["bronze"]),
        silver_schema=str(config.data["schemas"]["silver"]),
        silver_reject_schema=str(config.data["schemas"]["silver_reject"]),
        operations_schema=str(config.data["schemas"]["operations"]),
    )


def ensure_release_environment(
    spark: Any,
    config: BronzeToSilverConfig,
    create_missing: bool = True,
) -> ReleaseEnvironmentStatus:
    """Validate or create the schemas required for one Bronze-to-Silver release."""
    environment = resolve_release_environment(config)
    existing_schemas = _get_existing_schemas(spark, environment.catalog)

    schema_statuses = []
    for schema_name in (
        environment.bronze_schema,
        environment.silver_schema,
        environment.silver_reject_schema,
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
