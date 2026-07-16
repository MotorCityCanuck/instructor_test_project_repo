"""Reconciliation helpers for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReconciliationSummary:
    """Reconciliation counts for one Bronze-to-Silver table build."""

    bronze_row_count: int
    exact_duplicate_count: int
    business_key_loser_count: int
    rejected_row_count: int
    accepted_row_count: int
    reconciliation_difference: int
    status: str


def reconcile_table_counts(
    bronze_row_count: int,
    exact_duplicate_count: int,
    business_key_loser_count: int,
    rejected_row_count: int,
    accepted_row_count: int,
) -> ReconciliationSummary:
    """Reconcile Bronze input rows against Silver table outcomes."""
    reconciliation_difference = bronze_row_count - (
        exact_duplicate_count
        + business_key_loser_count
        + rejected_row_count
        + accepted_row_count
    )
    status = "PASSED" if reconciliation_difference == 0 else "FAILED"
    return ReconciliationSummary(
        bronze_row_count=bronze_row_count,
        exact_duplicate_count=exact_duplicate_count,
        business_key_loser_count=business_key_loser_count,
        rejected_row_count=rejected_row_count,
        accepted_row_count=accepted_row_count,
        reconciliation_difference=reconciliation_difference,
        status=status,
    )
