# Raw-to-Bronze Databricks Workflow

**Purpose:** This guide explains how to deploy, run, inspect, and rerun the instructor Raw-to-Bronze Databricks Workflow for the NAPA releases.

## Workflow Summary

The workflow is defined in `config/raw_to_bronze/workflows/napa_raw_to_bronze.job.yml`.

It exposes one job parameter:

```text
release_type
```

Allowed values are:

```text
5k
50k
250k
```

The workflow maps those values to the existing release configuration names:

| `release_type` | Configuration release |
|---|---|
| `5k` | `napa_5k` |
| `50k` | `napa_50k` |
| `250k` | `napa_250k` |

## Task Order

The job runs these Python script tasks in order:

```text
resolve_configuration
        |
validate_release_environment
        |
validate_raw_inventory
        |
build_bronze_tables
        |
validate_bronze_reconciliation
        |
finalize_pipeline_run
```

`finalize_pipeline_run` is configured with `run_if: ALL_DONE` so it can close a started audit record as failed when a later task fails. It does not mark a failed run as successful.

## Configuration Resolution

Release configuration lives under `config/raw_to_bronze/`:

- `base.yml`: shared pipeline, runtime, publication, metadata, and policy settings.
- `environments/napa_5k.yml`: 5K release schemas, raw volume path, and release metadata.
- `environments/napa_50k.yml`: 50K release schemas, raw volume path, and release metadata.
- `environments/napa_250k.yml`: 250K release schemas, raw volume path, and release metadata.
- `raw_sources.yml`: shared source file inventory and Bronze table mapping.
- `logging.yml`: shared logging configuration.

The three releases use the same pipeline code and source registry. Only release-specific configuration values differ.

## Run ID Propagation

`resolve_configuration` generates one pipeline run ID with the existing `create_pipeline_context` helper. It sets the workflow task value:

```python
dbutils.jobs.taskValues.set(key="run_id", value=context.pipeline_run_id)
```

Every downstream script task receives the same value through the workflow dynamic reference:

```text
{{tasks.resolve_configuration.values.run_id}}
```

The `validate_release_environment` task creates the operations tables if needed and writes the initial `RUNNING` record to `pipeline_runs`. The final task updates that same record.

## Deployment

The workflow YAML is written in Databricks Asset Bundle resource format. The repository root now contains `databricks.yml`, which includes `config/raw_to_bronze/workflows/*.yml` and syncs the pipeline source files required by the job tasks.

Required bundle variables:

```text
script_root  Workspace path to the synced repository notebooks directory
```

For Databricks Free Edition, the deployed job uses serverless compute. The workflow defines:

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

The root bundle sync includes:

- `notebooks/**`
- `src/**`
- `config/raw_to_bronze/**`
- `requirements.txt`

That keeps the Databricks Python script tasks deployable without any local Windows paths.

## Running the Workflow

From the Databricks UI:

1. Run `databricks bundle deploy -t dev`.
2. In the Databricks workspace UI, open **Workflows**.
3. Open the deployed `NAPA Raw to Bronze` job.
4. Click **Run now**.
5. Set `release_type` to one of `5k`, `50k`, or `250k`.
6. Start the run.

UI notes:

- `release_type` is a Databricks job parameter.
- The workflow runs the bundled Python script tasks, not notebooks.
- Redeploying the bundle updates the same job definition in the UI.

CLI examples after deployment:

```bash
databricks bundle run -t dev napa_raw_to_bronze --params release_type=5k
databricks bundle run -t dev napa_raw_to_bronze --params release_type=50k
databricks bundle run -t dev napa_raw_to_bronze --params release_type=250k
```

## Inspecting a Failed Run

Use the Databricks run page first:

- Identify the failed task.
- Open task logs for the exception and printed release/run details.
- Find the `run_id` in the `resolve_configuration` task output or task values.

Then inspect the operations schema configured for the release, currently `workspace.instructor_ops` unless configuration changes it:

```sql
SELECT *
FROM workspace.instructor_ops.pipeline_runs
WHERE pipeline_run_id = '<run_id>';

SELECT *
FROM workspace.instructor_ops.run_messages
WHERE pipeline_run_id = '<run_id>'
ORDER BY created_ts;

SELECT *
FROM workspace.instructor_ops.table_runs
WHERE pipeline_run_id = '<run_id>'
ORDER BY started_ts;

SELECT *
FROM workspace.instructor_ops.reconciliation_results
WHERE pipeline_run_id = '<run_id>'
ORDER BY source_file_name;
```

If failure occurs before `validate_release_environment` creates the operations schema and start record, use the failed task logs because no durable audit record may exist yet.

## Rerun Behavior

The pipeline uses a full-refresh Bronze publication mode. Rerunning the whole workflow generates a new `run_id` and overwrites the configured Bronze tables instead of appending duplicate Bronze rows.

For transient infrastructure failures after the run has started, rerun the failed task in Databricks only when the task receives the original task values, especially `run_id`. For data, inventory, schema, or reconciliation failures, correct the underlying issue and rerun the whole workflow for the selected `release_type`.

Do not manually invent or edit a `run_id`; it is created by `resolve_configuration` and propagated by the workflow.
