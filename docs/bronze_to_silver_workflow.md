# Bronze-to-Silver Databricks Workflow

**Purpose:** This guide explains the Bronze-to-Silver Databricks Workflow resource, its task graph, bundle wiring, and the current execution boundary in this repository.

## Workflow Summary

The workflow is defined in `config/bronze_to_silver/workflows/napa_bronze_to_silver.job.yml`.

It exposes one job parameter:

```text
release_name
```

Allowed values are:

```text
napa_5k
napa_50k
napa_250k
```

## Task Order

The job runs these Python script tasks in order:

```text
resolve_configuration
        |
validate_environment
        |
validate_bronze_sources
        |
build_reference
        |
build_athlete
        |
build_organization_partnership
        |
build_competition
        |
run_cross_table_validation
        |
publish_convenience_views
        |
finalize_pipeline_summary
```

`finalize_pipeline_summary` is configured with `run_if: ALL_DONE` so the workflow can always emit a final task result even if an earlier task fails.

## Deployment

The root bundle now includes both workflow families:

- `config/raw_to_bronze/workflows/*.yml`
- `config/bronze_to_silver/workflows/*.yml`

For Databricks Free Edition, the Bronze-to-Silver job uses serverless compute. The workflow defines:

```text
performance_target = STANDARD
environment_key    = napa_serverless_python
environment_version = 4
```

Each Python script task references that job-level serverless environment with `environment_key`.

Exact CLI commands:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Current Execution Boundary

This workflow resource and its script entrypoints now perform actual stage execution against the current instructor reference modules:

- resolve and validate Bronze-to-Silver configuration;
- validate the target schemas;
- validate the configured Bronze source-table inventory;
- execute the current reference, athlete, organization/partnership, and competition table builders;
- publish accepted Silver tables and reject tables;
- write table-run, reconciliation, quality, schema-snapshot, and run-message operations records;
- run cross-table validation;
- publish convenience SQL views;
- finalize the durable pipeline run status.

Within the shared `workspace.instructor_ops` schema, Bronze-to-Silver now writes to its own operations tables:

- `b2s_pipeline_runs`
- `b2s_table_runs`
- `b2s_quality_results`
- `b2s_reconciliation_results`
- `b2s_schema_snapshots`
- `b2s_run_messages`

One important constraint remains:

- the reference, athlete, organization/partnership, and competition stage builds now publish through Spark SQL;
- cross-table validation and convenience-view publication run through Spark SQL against published Silver tables, so those later workflow tasks no longer collect full Silver row sets back to the driver.

That means the workflow is now execution-capable and the Bronze-to-Silver stage-build path no longer depends on per-table Python row materialization for the configured Silver tables. Treat the current implementation as functionally wired and locally testable; deployment and workspace-scale validation still need to be run in Databricks.
