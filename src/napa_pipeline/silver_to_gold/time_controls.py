"""Analysis-date helpers for the Silver-to-Gold pipeline."""

from __future__ import annotations

from datetime import date, datetime


class AnalysisDateResolutionError(ValueError):
    """Raised when analysis_as_of_date cannot be resolved."""


def resolve_analysis_as_of_date(
    match_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    explicit_analysis_as_of_date: date | None = None,
) -> date:
    """Resolve the Gold analysis_as_of_date using MAX_VALID_MATCH_DATE."""
    if explicit_analysis_as_of_date is not None:
        return explicit_analysis_as_of_date

    valid_match_dates = [
        parsed_date
        for row in match_rows
        for parsed_date in [_parse_date_value(row.get("match_date"))]
        if parsed_date is not None
    ]
    if not valid_match_dates:
        raise AnalysisDateResolutionError(
            "Could not resolve analysis_as_of_date from matches using MAX_VALID_MATCH_DATE."
        )
    return max(valid_match_dates)


def _parse_date_value(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None
