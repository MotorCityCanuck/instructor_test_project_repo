"""Raw file inventory validation for the Raw-to-Bronze pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from napa_pipeline.raw_to_bronze.config import RawToBronzeConfig
from napa_pipeline.raw_to_bronze.environment import ReleaseEnvironment
from napa_pipeline.raw_to_bronze.operations import calculate_schema_hash


class RawInventoryError(RuntimeError):
    """Raised when Raw inventory validation fails."""


@dataclass(frozen=True)
class RawFileRecord:
    """Metadata captured for a raw file."""

    file_name: str
    file_path: str
    file_size: int | None
    modification_ts: datetime | None


@dataclass(frozen=True)
class RawInventoryStatus:
    """Validation outcome for Raw file inventory."""

    expected_files: tuple[str, ...]
    discovered_files: tuple[RawFileRecord, ...]
    missing_files: tuple[str, ...]
    unexpected_files: tuple[str, ...]
    policy: str


@dataclass(frozen=True)
class SourceReadinessRecord:
    """Readability and schema metadata for one configured source file."""

    source_name: str
    file_name: str
    file_path: str
    file_size: int | None
    modification_ts: datetime | None
    row_count: int
    schema_hash: str
    schema_fields: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class InventoryValidationResult:
    """Combined result for exact inventory and source readability checks."""

    inventory_status: RawInventoryStatus
    source_readiness: tuple[SourceReadinessRecord, ...]


def validate_raw_inventory(
    dbutils: Any,
    config: RawToBronzeConfig,
    environment: ReleaseEnvironment,
) -> RawInventoryStatus:
    """Validate exact Raw file inventory for a release volume."""
    entries = dbutils.fs.ls(environment.raw_volume_path)
    discovered_files = tuple(
        RawFileRecord(
            file_name=entry.name,
            file_path=entry.path,
            file_size=getattr(entry, "size", None),
            modification_ts=_normalize_modification_time(getattr(entry, "modificationTime", None)),
        )
        for entry in entries
    )

    expected_files = tuple(
        source["file_name"] for source in config.sources_in_build_order
    )
    discovered_names = {item.file_name for item in discovered_files}
    expected_names = set(expected_files)

    missing_files = tuple(sorted(expected_names - discovered_names))
    unexpected_files = tuple(sorted(discovered_names - expected_names))
    policy = str(config.data["execution"]["unexpected_file_policy"]).lower()

    if missing_files:
        raise RawInventoryError(
            f"Missing required Raw files: {', '.join(missing_files)}."
        )
    if unexpected_files and policy == "fail":
        raise RawInventoryError(
            f"Unexpected Raw files detected: {', '.join(unexpected_files)}."
        )

    return RawInventoryStatus(
        expected_files=expected_files,
        discovered_files=discovered_files,
        missing_files=missing_files,
        unexpected_files=unexpected_files,
        policy=policy,
    )


def validate_raw_inventory_and_readiness(
    spark: Any,
    dbutils: Any,
    config: RawToBronzeConfig,
    environment: ReleaseEnvironment,
) -> InventoryValidationResult:
    """Validate exact inventory and confirm every configured source is readable."""
    inventory_status = validate_raw_inventory(dbutils, config, environment)
    source_readiness = validate_source_readiness(spark, inventory_status, config)
    return InventoryValidationResult(
        inventory_status=inventory_status,
        source_readiness=tuple(source_readiness),
    )


def validate_source_readiness(
    spark: Any,
    inventory_status: RawInventoryStatus,
    config: RawToBronzeConfig,
) -> list[SourceReadinessRecord]:
    """Validate that each configured source is readable Parquet and capture schema metadata."""
    discovered_by_name = {
        record.file_name: record for record in inventory_status.discovered_files
    }
    readiness_records: list[SourceReadinessRecord] = []

    for source in config.sources_in_build_order:
        file_record = discovered_by_name[source["file_name"]]
        try:
            dataframe = spark.read.parquet(file_record.file_path)
            schema_fields = tuple(_spark_schema_to_fields(dataframe.schema))
            readiness_records.append(
                SourceReadinessRecord(
                    source_name=source["source_name"],
                    file_name=file_record.file_name,
                    file_path=file_record.file_path,
                    file_size=file_record.file_size,
                    modification_ts=file_record.modification_ts,
                    row_count=dataframe.count(),
                    schema_hash=calculate_schema_hash(list(schema_fields)),
                    schema_fields=schema_fields,
                )
            )
        except Exception as exc:
            raise RawInventoryError(
                "Configured Raw source could not be read as Parquet: "
                f"{file_record.file_name} ({file_record.file_path})."
            ) from exc

    return readiness_records


def _spark_schema_to_fields(schema: Any) -> list[dict[str, Any]]:
    return [
        {
            "column_name": field.name,
            "data_type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in schema.fields
    ]


def _normalize_modification_time(modification_time: Any) -> datetime | None:
    if modification_time in (None, ""):
        return None
    if isinstance(modification_time, datetime):
        return modification_time
    return datetime.fromtimestamp(int(modification_time) / 1000)
