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

This workflow resource and its script entrypoints are bundle-valid and ready for deployment.

At the current repository step, the task scripts do the following:

- resolve and validate Bronze-to-Silver configuration;
- validate the target schemas;
- validate the configured Bronze source-table inventory;
- expose the configured table stages and convenience-view registrations.

They do **not yet** publish Silver tables or Databricks SQL views end to end. The actual Databricks stage-execution and publication wiring remains a follow-up implementation step.

That boundary is intentional in the current repository state. Do not treat a successful local bundle validation as proof that the Bronze-to-Silver Databricks execution path is complete.
