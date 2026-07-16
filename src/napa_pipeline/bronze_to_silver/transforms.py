"""Reusable standardization helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any


NON_ALNUM_PATTERN = re.compile(r"[^0-9A-Za-z]+")
MULTISPACE_PATTERN = re.compile(r"\s+")


def to_snake_case(column_name: str) -> str:
    """Convert a source column name to snake_case."""
    normalized = NON_ALNUM_PATTERN.sub("_", column_name.strip())
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_").lower()


def standardize_string(value: Any, *, uppercase: bool = False) -> str | None:
    """Trim, normalize whitespace, and optionally uppercase a string value."""
    if value is None:
        return None
    text = str(value).replace("\r\n", "\n").replace("\r", "\n").strip()
    text = MULTISPACE_PATTERN.sub(" ", text)
    if not text:
        return None
    return text.upper() if uppercase else text


def normalize_domain_value(
    value: Any,
    domain_config: dict[str, Any],
) -> str | None:
    """Normalize a coded value using allowed values and configured synonyms."""
    normalized = standardize_string(value, uppercase=True)
    if normalized is None:
        return None

    synonyms = {
        str(key).upper(): str(mapped_value).upper()
        for key, mapped_value in domain_config.get("synonyms", {}).items()
    }
    allowed = {str(item).upper() for item in domain_config.get("allowed", [])}
    candidate = synonyms.get(normalized, normalized)
    return candidate if candidate in allowed else None


def safe_cast_int(value: Any) -> int | None:
    """Safely cast a scalar value to int."""
    if value is None or value == "":
        return None
    return int(str(value))


def safe_cast_float(value: Any) -> float | None:
    """Safely cast a scalar value to float."""
    if value is None or value == "":
        return None
    return float(str(value))


def safe_cast_decimal(value: Any) -> Decimal | None:
    """Safely cast a scalar value to Decimal."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value '{value}'.") from exc


def safe_cast_date(value: Any) -> date | None:
    """Safely cast an ISO-like date string to a date."""
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value))


def safe_cast_timestamp(value: Any) -> datetime | None:
    """Safely cast an ISO-like timestamp string to a datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
