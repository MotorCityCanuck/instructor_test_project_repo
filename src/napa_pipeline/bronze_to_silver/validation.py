"""Validation helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class SourceContractValidationResult:
    """Result of validating a Bronze source contract against expected columns."""

    missing_required_columns: tuple[str, ...]
    unexpected_columns: tuple[str, ...]
    status: str


def validate_source_contract(
    source_columns: Iterable[str],
    required_columns: Iterable[str],
    expected_columns: Iterable[str],
) -> SourceContractValidationResult:
    """Validate required and unexpected column sets for one Bronze source."""
    source_set = {column for column in source_columns}
    required_set = {column for column in required_columns}
    expected_set = {column for column in expected_columns}

    missing_required = tuple(sorted(required_set - source_set))
    unexpected = tuple(sorted(source_set - expected_set))
    status = "FAILED" if missing_required else "SUCCEEDED"
    return SourceContractValidationResult(
        missing_required_columns=missing_required,
        unexpected_columns=unexpected,
        status=status,
    )


def calculate_failure_pct(
    evaluated_row_count: int | None,
    failed_row_count: int | None,
) -> float | None:
    """Calculate failure percentage for a quality rule result."""
    if not evaluated_row_count:
        return None
    if failed_row_count is None:
        return None
    return round((failed_row_count / evaluated_row_count) * 100, 4)


def validate_required_fields(
    record: dict[str, object],
    required_fields: Iterable[str],
) -> list[str]:
    """Return required fields that are null or empty after standardization."""
    failed_fields = []
    for field_name in required_fields:
        value = record.get(field_name)
        if value is None or value == "":
            failed_fields.append(field_name)
    return failed_fields
