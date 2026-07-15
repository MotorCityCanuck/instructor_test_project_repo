"""Notebook helper for importing repo-based pipeline modules in Databricks."""

# Databricks notebook source

# COMMAND ----------
HELPER_VERSION = "2026.07.15.1"

from pathlib import Path
import sys


def bootstrap_napa_pipeline_imports() -> None:
    """Add the repository src directory to sys.path for notebook execution."""
    search_roots = []

    if "__file__" in globals():
        search_roots.append(Path(__file__).resolve().parent)

    current_dir = Path.cwd().resolve()
    search_roots.extend([current_dir, *current_dir.parents])

    for root in search_roots:
        for candidate in (root / "src", root / "notebooks" / ".." / "src"):
            normalized = candidate.resolve()
            package_path = normalized / "napa_pipeline"
            if package_path.exists():
                normalized_str = str(normalized)
                if normalized_str not in sys.path:
                    sys.path.insert(0, normalized_str)
                return

    raise ModuleNotFoundError(
        "Could not locate the repository src/napa_pipeline package. "
        "Run this notebook from the synced repository workspace."
    )
