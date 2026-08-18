# Full Pipeline Orchestration Workflow

**Purpose:** This guide explains the top-level Databricks orchestration workflow that chains raw certification, raw-to-bronze, bronze-to-silver, and silver-to-gold in a single release run with explicit success dependencies.

## Workflow Summary

The workflow is defined in `config/orchestration/workflows/napa_full_pipeline_orchestration.job.yml`.

It exposes two job parameters:

```text
release_name
analysis_as_of_date
```

Allowed `release_name` values are:

```text
napa_5k
napa_50k
napa_250k
```

`analysis_as_of_date` is optional. Leave it empty to let downstream workflows resolve their default as-of date behavior.

## Task Progression

The orchestration job evaluates the selected release and then runs exactly one release-specific branch. Within that branch, each downstream job waits for the prior job to succeed:

```text
raw_certification
        |
raw_to_bronze
        |
bronze_to_silver
        |
silver_to_gold
```

This means:

- `raw_to_bronze` does not start unless raw certification succeeds.
- `bronze_to_silver` does not start unless raw-to-bronze succeeds.
- `silver_to_gold` does not start unless bronze-to-silver succeeds.
- there is no parallel stage progression across the medallion layers in this orchestration job.

## Parameter Mapping

The orchestration job accepts `release_name`, but the current underlying jobs do not all use the same parameter shape:

- `napa_raw_certification` expects `release_type = 5k | 50k | 250k`
- `napa_raw_to_bronze` expects `release_type = 5k | 50k | 250k`
- `napa_bronze_to_silver` expects `release_name = napa_5k | napa_50k | napa_250k`
- `napa_silver_to_gold` expects `release_name = napa_5k | napa_50k | napa_250k`

The orchestration workflow handles that mapping internally by branching on `release_name` and passing the appropriate downstream parameter values for the selected release.

## Deployment

The root bundle includes all implemented workflow directories, including the orchestration workflow:

- `config/raw_to_bronze/workflows/*.yml`
- `config/bronze_to_silver/workflows/*.yml`
- `config/silver_to_gold/workflows/*.yml`
- `config/certification/workflows/*.yml`
- `config/orchestration/workflows/*.yml`

Exact CLI commands:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Run Commands

Examples:

```bash
databricks bundle run -t dev napa_full_pipeline_orchestration --params release_name=napa_5k
databricks bundle run -t dev napa_full_pipeline_orchestration --params release_name=napa_50k
databricks bundle run -t dev napa_full_pipeline_orchestration --params release_name=napa_250k
databricks bundle run -t dev napa_full_pipeline_orchestration --params release_name=napa_5k,analysis_as_of_date=2025-12-31
```

## Operational Notes

- This workflow is an orchestration layer only. The implementation logic remains in the existing underlying jobs.
- A failed upstream job stops progression to the next stage because the downstream `run_job_task` depends on upstream success.
- The workflow does not currently include the standalone Gold audit job. Run `napa_silver_to_gold_audit` separately after a successful orchestration run when audit validation is needed.
