"""Source loading helpers for Raw certification inventory checks."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import PurePosixPath
from typing import Any

from napa_pipeline.certification.config import CertificationConfig
from napa_pipeline.certification.environment import ReleaseEnvironment
from napa_pipeline.certification.models import (
    CertificationRuleResult,
    DiscoveredFileRecord,
    InventoryCertificationResult,
    ManifestRecord,
    SourceContract,
    SourceLoadResult,
)
from napa_pipeline.raw_to_bronze.operations import calculate_schema_hash


class CertificationSourceError(RuntimeError):
    """Raised when Raw certification source loading cannot proceed."""


def run_inventory_certification(
    spark: Any,
    dbutils: Any,
    config: CertificationConfig,
    environment: ReleaseEnvironment,
) -> InventoryCertificationResult:
    """Load the Raw release inventory, source metadata, and inventory-rule results."""
    expected_sources = tuple(
        SourceContract(
            source_name=source["source_name"],
            file_name=source["file_name"],
            key_columns=tuple(source["key_columns"]),
            build_order=int(source["build_order"]),
        )
        for source in config.sources_in_build_order
    )

    try:
        entries = dbutils.fs.ls(environment.raw_volume_path)
        path_exists = True
        path_error = None
    except Exception as exc:
        entries = []
        path_exists = False
        path_error = str(exc)

    discovered_files = tuple(
        DiscoveredFileRecord(
            file_name=_normalize_entry_name(entry),
            file_path=entry.path,
            file_size=getattr(entry, "size", None),
            modification_ts=_normalize_modification_time(
                getattr(entry, "modificationTime", None)
            ),
        )
        for entry in entries
    )

    manifest = _load_manifest_if_present(dbutils, config, environment, discovered_files)

    if not path_exists:
        rule_results = (
            CertificationRuleResult(
                rule_id="RAW_PATH_EXISTS",
                name="Raw release path exists",
                pillar="Release Inventory and Format",
                category="inventory",
                status="FAIL",
                severity="blocker",
                message=(
                    f"Configured Raw release path could not be listed: "
                    f"{environment.raw_volume_path}. {path_error or ''}".strip()
                ),
            ),
        )
        return InventoryCertificationResult(
            release_name=config.release_name,
            release_path=environment.raw_volume_path,
            source_mode="parquet",
            path_exists=False,
            expected_sources=expected_sources,
            discovered_files=discovered_files,
            loaded_sources=(),
            manifest=manifest,
            rule_results=rule_results,
        )

    loaded_sources = tuple(_load_source_records(spark, expected_sources, discovered_files))
    rule_results = evaluate_inventory_rules(
        config.release_name,
        environment.raw_volume_path,
        expected_sources,
        discovered_files,
        loaded_sources,
        manifest,
    )
    return InventoryCertificationResult(
        release_name=config.release_name,
        release_path=environment.raw_volume_path,
        source_mode="parquet",
        path_exists=True,
        expected_sources=expected_sources,
        discovered_files=discovered_files,
        loaded_sources=loaded_sources,
        manifest=manifest,
        rule_results=rule_results,
    )


def evaluate_inventory_rules(
    release_name: str,
    release_path: str,
    expected_sources: tuple[SourceContract, ...],
    discovered_files: tuple[DiscoveredFileRecord, ...],
    loaded_sources: tuple[SourceLoadResult, ...],
    manifest: ManifestRecord | None,
) -> tuple[CertificationRuleResult, ...]:
    """Evaluate inventory and readability rules for one release."""
    results: list[CertificationRuleResult] = [
        CertificationRuleResult(
            rule_id="RAW_RELEASE_NAME_VALID",
            name="Release name is valid",
            pillar="Release Inventory and Format",
            category="inventory",
            status="PASS",
            severity="info",
            message=f"Release '{release_name}' normalized successfully.",
            observed_value=release_name,
        ),
        CertificationRuleResult(
            rule_id="RAW_PATH_EXISTS",
            name="Raw release path exists",
            pillar="Release Inventory and Format",
            category="inventory",
            status="PASS",
            severity="info",
            message=f"Configured Raw release path exists: {release_path}.",
            observed_value=release_path,
        ),
    ]

    expected_file_names = tuple(source.file_name for source in expected_sources)
    discovered_name_counts: dict[str, int] = {}
    for record in discovered_files:
        discovered_name_counts[record.file_name] = discovered_name_counts.get(record.file_name, 0) + 1

    missing_files = tuple(
        file_name for file_name in expected_file_names if file_name not in discovered_name_counts
    )
    duplicate_files = tuple(
        sorted(file_name for file_name, count in discovered_name_counts.items() if count > 1)
    )

    results.append(
        CertificationRuleResult(
            rule_id="RAW_REQUIRED_FILES_PRESENT",
            name="Required Raw files are present",
            pillar="Release Inventory and Format",
            category="inventory",
            status="PASS" if not missing_files else "FAIL",
            severity="info" if not missing_files else "blocker",
            message=(
                "All required Raw files are present."
                if not missing_files
                else f"Missing required Raw files: {', '.join(missing_files)}."
            ),
            affected_count=len(missing_files),
            sample_records=missing_files[:5],
        )
    )

    results.append(
        CertificationRuleResult(
            rule_id="RAW_UNEXPECTED_DUPLICATE_DOMAIN",
            name="Duplicate Raw source domain files are not present",
            pillar="Release Inventory and Format",
            category="inventory",
            status="PASS" if not duplicate_files else "FAIL",
            severity="info" if not duplicate_files else "error",
            message=(
                "No duplicate Raw domain files were discovered."
                if not duplicate_files
                else f"Duplicate Raw domain files discovered: {', '.join(duplicate_files)}."
            ),
            affected_count=len(duplicate_files),
            sample_records=duplicate_files[:5],
        )
    )

    unreadable_sources = tuple(
        source.file_name for source in loaded_sources if source.read_status == "UNREADABLE"
    )
    empty_sources = tuple(
        source.file_name for source in loaded_sources if source.read_status == "EMPTY"
    )

    results.append(
        CertificationRuleResult(
            rule_id="RAW_PARQUET_READABLE",
            name="Required Raw Parquet sources are readable",
            pillar="Release Inventory and Format",
            category="inventory",
            status="PASS" if not unreadable_sources else "FAIL",
            severity="info" if not unreadable_sources else "blocker",
            message=(
                "All required Raw Parquet sources were readable."
                if not unreadable_sources
                else f"Unreadable Raw sources: {', '.join(unreadable_sources)}."
            ),
            affected_count=len(unreadable_sources),
            sample_records=unreadable_sources[:5],
        )
    )

    results.append(
        CertificationRuleResult(
            rule_id="RAW_NONEMPTY_SOURCE",
            name="Required Raw sources are non-empty",
            pillar="Release Inventory and Format",
            category="inventory",
            status="PASS" if not empty_sources else "FAIL",
            severity="info" if not empty_sources else "blocker",
            message=(
                "All required Raw sources contained at least one record."
                if not empty_sources
                else f"Empty Raw sources: {', '.join(empty_sources)}."
            ),
            affected_count=len(empty_sources),
            sample_records=empty_sources[:5],
        )
    )

    results.append(_evaluate_manifest_match(expected_file_names, loaded_sources, manifest))
    return tuple(results)


def _load_source_records(
    spark: Any,
    expected_sources: tuple[SourceContract, ...],
    discovered_files: tuple[DiscoveredFileRecord, ...],
) -> list[SourceLoadResult]:
    discovered_by_name: dict[str, list[DiscoveredFileRecord]] = {}
    for record in discovered_files:
        discovered_by_name.setdefault(record.file_name, []).append(record)

    source_results: list[SourceLoadResult] = []
    for source in expected_sources:
        file_records = discovered_by_name.get(source.file_name, [])
        if len(file_records) != 1:
            continue

        file_record = file_records[0]
        try:
            dataframe = spark.read.parquet(file_record.file_path)
            schema_fields = tuple(_spark_schema_to_fields(dataframe.schema))
            row_count = int(dataframe.count())
            temp_view_name = f"raw_cert_{source.source_name}"
            if hasattr(dataframe, "createOrReplaceTempView"):
                dataframe.createOrReplaceTempView(temp_view_name)
            source_results.append(
                SourceLoadResult(
                    source_name=source.source_name,
                    file_name=file_record.file_name,
                    file_path=file_record.file_path,
                    file_size=file_record.file_size,
                    modification_ts=file_record.modification_ts,
                    row_count=row_count,
                    schema_hash=calculate_schema_hash(list(schema_fields)),
                    schema_fields=schema_fields,
                    temp_view_name=temp_view_name,
                    read_status="READY" if row_count > 0 else "EMPTY",
                )
            )
        except Exception as exc:
            source_results.append(
                SourceLoadResult(
                    source_name=source.source_name,
                    file_name=file_record.file_name,
                    file_path=file_record.file_path,
                    file_size=file_record.file_size,
                    modification_ts=file_record.modification_ts,
                    row_count=None,
                    schema_hash=None,
                    schema_fields=(),
                    temp_view_name=None,
                    read_status="UNREADABLE",
                    error_message=str(exc),
                )
            )
    return source_results


def _evaluate_manifest_match(
    expected_file_names: tuple[str, ...],
    loaded_sources: tuple[SourceLoadResult, ...],
    manifest: ManifestRecord | None,
) -> CertificationRuleResult:
    if manifest is None:
        return CertificationRuleResult(
            rule_id="RAW_MANIFEST_ROW_COUNT_MATCH",
            name="Manifest row counts reconcile to loaded sources",
            pillar="Release Inventory and Format",
            category="manifest",
            status="WARN",
            severity="warning",
            message="No release manifest was found; row-count reconciliation was skipped.",
        )

    manifest_counts = manifest.payload.get("file_row_counts") or {}
    mismatches: list[str] = []
    loaded_by_file = {source.file_name: source for source in loaded_sources}
    for file_name in expected_file_names:
        if file_name not in manifest_counts or file_name not in loaded_by_file:
            continue
        observed = loaded_by_file[file_name].row_count
        expected = manifest_counts[file_name]
        if observed != expected:
            mismatches.append(f"{file_name}: observed={observed}, expected={expected}")

    return CertificationRuleResult(
        rule_id="RAW_MANIFEST_ROW_COUNT_MATCH",
        name="Manifest row counts reconcile to loaded sources",
        pillar="Release Inventory and Format",
        category="manifest",
        status="PASS" if not mismatches else "FAIL",
        severity="info" if not mismatches else "error",
        message=(
            "Manifest row counts matched loaded source counts."
            if not mismatches
            else "Manifest row-count mismatches detected."
        ),
        affected_count=len(mismatches),
        sample_records=tuple(mismatches[:5]),
    )


def _load_manifest_if_present(
    dbutils: Any,
    config: CertificationConfig,
    environment: ReleaseEnvironment,
    discovered_files: tuple[DiscoveredFileRecord, ...],
) -> ManifestRecord | None:
    if not bool(config.data["manifest"].get("enabled", True)):
        return None

    candidate_names = tuple(config.data["manifest"].get("file_names", ()))
    discovered_by_name = {record.file_name: record for record in discovered_files}
    for file_name in candidate_names:
        record = discovered_by_name.get(file_name)
        if record is None:
            continue
        payload = _read_json_text(dbutils, record.file_path)
        if payload is None:
            continue
        return ManifestRecord(
            file_name=file_name,
            file_path=record.file_path,
            payload=payload,
        )
    return None


def _read_json_text(dbutils: Any, file_path: str) -> dict[str, Any] | None:
    try:
        contents = dbutils.fs.head(file_path, 1024 * 1024)
    except Exception:
        return None
    try:
        return json.loads(contents)
    except Exception:
        return None


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


def _normalize_entry_name(entry: Any) -> str:
    raw_name = str(getattr(entry, "name", "") or "").rstrip("/")
    if raw_name:
        return raw_name
    path_value = str(getattr(entry, "path", "") or "").rstrip("/")
    return PurePosixPath(path_value).name
