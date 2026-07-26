# Medallion Design

**Purpose:** This document summarizes the implemented medallion-layer boundaries for the instructor reference NAPA pipelines.

## Implemented Layer Boundaries

| Layer | Implemented purpose | Physical location | Current workflow |
|---|---|---|---|
| Raw | Immutable source Parquet files in release-specific Unity Catalog Volumes. | `workspace.instructor_<scale>_raw.napa_files` | Source files are validated by `napa_raw_to_bronze`. |
| Bronze | Source-aligned Delta tables that preserve Raw rows and business columns while adding ingestion metadata. | `workspace.instructor_<scale>_bronze` | Published by `napa_raw_to_bronze`. |
| Silver | Conformed operational entities, reject tables, quality evidence, cross-table checks, and convenience views. | `workspace.instructor_<scale>_silver` and `workspace.instructor_<scale>_silver_reject` | Published by `napa_bronze_to_silver`. |
| Gold | Business-ready analytical tables for competition foundations, ratings, features, quality confidence, modeling, scorecards, recommendations, sensitivity, and explanations. | `workspace.instructor_<scale>_gold` and `workspace.instructor_<scale>_gold_stage` | Published by `napa_silver_to_gold`. |
| Operations | Durable run, table, schema, quality, reconciliation, model, recommendation, and audit evidence. | `workspace.instructor_ops` | Written by all implemented workflows. |

`<scale>` resolves to `5k`, `50k`, or `250k`.

## Release Isolation

The implementation uses one shared codebase and one shared operations schema, but separates business data by release-specific medallion schemas:

| Release | Raw | Bronze | Silver | Gold |
|---|---|---|---|---|
| `napa_5k` | `instructor_5k_raw` | `instructor_5k_bronze` | `instructor_5k_silver` | `instructor_5k_gold` |
| `napa_50k` | `instructor_50k_raw` | `instructor_50k_bronze` | `instructor_50k_silver` | `instructor_50k_gold` |
| `napa_250k` | `instructor_250k_raw` | `instructor_250k_bronze` | `instructor_250k_silver` | `instructor_250k_gold` |

## Data Model References

Use [implemented_layer_catalog.md](implemented_layer_catalog.md) for the implemented table catalog across Raw, Bronze, Silver, Gold, and operations.

Column-level references:

- Bronze and Silver validation export: [napa_5k_bronze_silver_columns.csv](napa_5k_bronze_silver_columns.csv)
- Gold table registry: [gold_target_schema_registry.md](gold_target_schema_registry.md)

## Current Constraints

- All implemented pipelines are full refresh.
- Workflow resources use Databricks serverless Python task configuration.
- Gold `analysis_as_of_date` resolves from the maximum valid Silver match date unless passed explicitly.
- Gold `gold_run_summary` remains planned but is not yet materialized by the Phase 3 through Phase 13 workflow.
- Student-facing placeholder documents remain in this repo, but this instructor branch now contains executable reference pipeline implementations.
