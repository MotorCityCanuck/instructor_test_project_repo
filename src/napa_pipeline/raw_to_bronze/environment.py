"""Environment setup and validation helpers for the Raw-to-Bronze pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from napa_pipeline.raw_to_bronze.config import RawToBronzeConfig


class EnvironmentValidationError(RuntimeError):
    """Raised when the Databricks release environment is invalid."""


@dataclass(frozen=True)
class ManagedObjectStatus:
    """Status for a schema or volume required by the pipeline."""

    object_type: str
    object_name: str
    existed: bool


@dataclass(frozen=True)
class ReleaseEnvironment:
    """Resolved Databricks object names for a release."""

    catalog: str
    raw_schema: str
    bronze_schema: str
    operations_schema: str
    raw_volume_name: str
    raw_volume_path: str

    @property
    def raw_volume_fqn(self) -> str:
        return f"{self.catalog}.{self.raw_schema}.{self.raw_volume_name}"


@dataclass(frozen=True)
class ReleaseEnvironmentStatus:
    """Outcome of validating or creating release-specific environment objects."""

    release_environment: ReleaseEnvironment
    schema_statuses: tuple[ManagedObjectStatus, ...]
    volume_status: ManagedObjectStatus


def resolve_release_environment(config: RawToBronzeConfig) -> ReleaseEnvironment:
    """Resolve release-specific schema and volume names from config."""
    return ReleaseEnvironment(
        catalog=str(config.data["runtime"]["catalog"]),
        raw_schema=str(config.data["schemas"]["raw"]),
        bronze_schema=str(config.data["schemas"]["bronze"]),
        operations_schema=str(config.data["schemas"]["operations"]),
        raw_volume_name=str(config.data["volume"]["name"]),
        raw_volume_path=str(config.data["volume"]["path"]),
    )


def ensure_release_environment(
    spark: Any,
    config: RawToBronzeConfig,
    create_missing: bool = True,
) -> ReleaseEnvironmentStatus:
    """Validate or create the schemas and raw volume required for one release."""
    environment = resolve_release_environment(config)
    existing_schemas = _get_existing_schemas(spark, environment.catalog)

    schema_statuses = []
    for schema_name in (
        environment.raw_schema,
        environment.bronze_schema,
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

    existing_volumes = _get_existing_volumes(
        spark,
        environment.catalog,
        environment.raw_schema,
    )
    volume_existed = environment.raw_volume_name in existing_volumes
    if not volume_existed:
        if not create_missing:
            raise EnvironmentValidationError(
                f"Required raw volume does not exist: {environment.raw_volume_fqn}"
            )
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {environment.raw_volume_fqn}")

    volume_status = ManagedObjectStatus(
        object_type="volume",
        object_name=environment.raw_volume_fqn,
        existed=volume_existed,
    )

    return ReleaseEnvironmentStatus(
        release_environment=environment,
        schema_statuses=tuple(schema_statuses),
        volume_status=volume_status,
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


def _get_existing_volumes(spark: Any, catalog: str, schema: str) -> set[str]:
    """Return existing volume names for the given schema."""
    try:
        rows = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}").collect()
    except Exception as exc:
        raise EnvironmentValidationError(
            f"Could not access volumes in '{catalog}.{schema}'."
        ) from exc

    volume_names = set()
    for row in rows:
        mapping = row.asDict() if hasattr(row, "asDict") else dict(row)
        for key in ("volume_name", "volumeName", "name"):
            value = mapping.get(key)
            if value:
                volume_names.add(str(value))
                break
    return volume_names
