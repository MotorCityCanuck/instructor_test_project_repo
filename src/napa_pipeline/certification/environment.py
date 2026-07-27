"""Environment setup and validation helpers for the Raw certification pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from napa_pipeline.certification.config import CertificationConfig


class CertificationEnvironmentError(RuntimeError):
    """Raised when the Databricks certification environment is invalid."""


@dataclass(frozen=True)
class ManagedObjectStatus:
    """Status for a schema or volume required by certification."""

    object_type: str
    object_name: str
    existed: bool


@dataclass(frozen=True)
class ReleaseEnvironment:
    """Resolved Databricks object names for one certification release."""

    catalog: str
    raw_schema: str
    operations_schema: str
    raw_volume_name: str
    raw_volume_path: str
    artifacts_volume_name: str
    artifacts_volume_path: str
    artifacts_root_path: str

    @property
    def raw_volume_fqn(self) -> str:
        return f"{self.catalog}.{self.raw_schema}.{self.raw_volume_name}"

    @property
    def artifacts_volume_fqn(self) -> str:
        return f"{self.catalog}.{self.operations_schema}.{self.artifacts_volume_name}"


@dataclass(frozen=True)
class ReleaseEnvironmentStatus:
    """Outcome of validating or creating certification environment objects."""

    release_environment: ReleaseEnvironment
    schema_statuses: tuple[ManagedObjectStatus, ...]
    volume_statuses: tuple[ManagedObjectStatus, ...]


def resolve_release_environment(config: CertificationConfig) -> ReleaseEnvironment:
    """Resolve release-specific schemas and volumes from config."""
    return ReleaseEnvironment(
        catalog=str(config.data["runtime"]["catalog"]),
        raw_schema=str(config.data["schemas"]["raw"]),
        operations_schema=str(config.data["schemas"]["operations"]),
        raw_volume_name=str(config.data["volumes"]["raw"]["name"]),
        raw_volume_path=str(config.data["volumes"]["raw"]["path"]),
        artifacts_volume_name=str(config.data["volumes"]["artifacts"]["name"]),
        artifacts_volume_path=str(config.data["volumes"]["artifacts"]["path"]),
        artifacts_root_path=str(config.data["artifacts"]["root_path"]),
    )


def ensure_release_environment(
    spark: Any,
    config: CertificationConfig,
    create_missing: bool = True,
) -> ReleaseEnvironmentStatus:
    """Validate or create the schemas and volumes required for certification."""
    environment = resolve_release_environment(config)
    existing_schemas = _get_existing_schemas(spark, environment.catalog)

    raw_schema_exists = environment.raw_schema in existing_schemas
    if not raw_schema_exists:
        raise CertificationEnvironmentError(
            f"Required raw schema does not exist: {environment.catalog}.{environment.raw_schema}"
        )

    operations_schema_existed = environment.operations_schema in existing_schemas
    if not operations_schema_existed:
        if not create_missing:
            raise CertificationEnvironmentError(
                f"Required operations schema does not exist: "
                f"{environment.catalog}.{environment.operations_schema}"
            )
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {environment.catalog}.{environment.operations_schema}"
        )

    schema_statuses = (
        ManagedObjectStatus(
            object_type="schema",
            object_name=f"{environment.catalog}.{environment.raw_schema}",
            existed=True,
        ),
        ManagedObjectStatus(
            object_type="schema",
            object_name=f"{environment.catalog}.{environment.operations_schema}",
            existed=operations_schema_existed,
        ),
    )

    raw_volumes = _get_existing_volumes(spark, environment.catalog, environment.raw_schema)
    if environment.raw_volume_name not in raw_volumes:
        raise CertificationEnvironmentError(
            f"Required raw volume does not exist: {environment.raw_volume_fqn}"
        )

    operations_volumes = _get_existing_volumes(
        spark,
        environment.catalog,
        environment.operations_schema,
    )
    artifacts_volume_existed = environment.artifacts_volume_name in operations_volumes
    if not artifacts_volume_existed:
        if not create_missing:
            raise CertificationEnvironmentError(
                f"Required artifacts volume does not exist: {environment.artifacts_volume_fqn}"
            )
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {environment.artifacts_volume_fqn}")

    volume_statuses = (
        ManagedObjectStatus(
            object_type="volume",
            object_name=environment.raw_volume_fqn,
            existed=True,
        ),
        ManagedObjectStatus(
            object_type="volume",
            object_name=environment.artifacts_volume_fqn,
            existed=artifacts_volume_existed,
        ),
    )

    return ReleaseEnvironmentStatus(
        release_environment=environment,
        schema_statuses=schema_statuses,
        volume_statuses=volume_statuses,
    )


def _get_existing_schemas(spark: Any, catalog: str) -> set[str]:
    """Return existing schema names for the given catalog."""
    try:
        rows = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
    except Exception as exc:
        raise CertificationEnvironmentError(
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
        raise CertificationEnvironmentError(
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

