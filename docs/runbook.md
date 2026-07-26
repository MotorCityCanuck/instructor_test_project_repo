# Runbook

**Purpose:** This document provides the current implementation runbook for deploying, running, validating, and inspecting the instructor NAPA medallion pipelines.

## Prerequisites

- Databricks CLI configured for the target workspace.
- Access to the `workspace` catalog.
- Raw files uploaded to the release-specific `napa_files` Volume before running Raw-to-Bronze.
- Repository changes deployed through Databricks Asset Bundles.
- Databricks Free Edition workflow tasks use serverless Python environments.

## Bundle Commands

Validate and deploy:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

The root bundle includes workflow resources under:

```text
config/raw_to_bronze/workflows/*.yml
config/bronze_to_silver/workflows/*.yml
config/silver_to_gold/workflows/*.yml
```

## Release Progression

Recommended validation order:

```text
napa_5k
  -> napa_50k
  -> napa_250k
```

Do not scale to the next release while the smaller release has unresolved structural failures.

## Run Raw-To-Bronze

Workflow resource:

```text
napa_raw_to_bronze
```

Parameters:

```text
release_type = 5k | 50k | 250k
```

CLI examples:

```bash
databricks bundle run -t dev napa_raw_to_bronze --params release_type=5k
databricks bundle run -t dev napa_raw_to_bronze --params release_type=50k
databricks bundle run -t dev napa_raw_to_bronze --params release_type=250k
```

Detailed guide:

```text
docs/raw_to_bronze_workflow.md
```

## Run Bronze-To-Silver

Workflow resource:

```text
napa_bronze_to_silver
```

Parameters:

```text
release_name = napa_5k | napa_50k | napa_250k
```

CLI examples:

```bash
databricks bundle run -t dev napa_bronze_to_silver --params release_name=napa_5k
databricks bundle run -t dev napa_bronze_to_silver --params release_name=napa_50k
databricks bundle run -t dev napa_bronze_to_silver --params release_name=napa_250k
```

Detailed guide:

```text
docs/bronze_to_silver_workflow.md
```

## Run Silver-To-Gold

Workflow resource:

```text
napa_silver_to_gold
```

Parameters:

```text
release_name = napa_5k | napa_50k | napa_250k
analysis_as_of_date = optional YYYY-MM-DD
```

CLI examples:

```bash
databricks bundle run -t dev napa_silver_to_gold --params release_name=napa_5k
databricks bundle run -t dev napa_silver_to_gold --params release_name=napa_50k
databricks bundle run -t dev napa_silver_to_gold --params release_name=napa_250k
databricks bundle run -t dev napa_silver_to_gold --params release_name=napa_5k,analysis_as_of_date=2025-12-31
```

Detailed guide:

```text
docs/silver_to_gold_workflow.md
```

## Run Standalone Gold Audit

Workflow resource:

```text
napa_silver_to_gold_audit
```

Run this after a successful Silver-to-Gold workflow for the same release and analysis date.

Parameters:

```text
release_name = napa_5k | napa_50k | napa_250k
analysis_as_of_date = optional YYYY-MM-DD
```

CLI examples:

```bash
databricks bundle run -t dev napa_silver_to_gold_audit --params release_name=napa_5k
databricks bundle run -t dev napa_silver_to_gold_audit --params release_name=napa_50k
databricks bundle run -t dev napa_silver_to_gold_audit --params release_name=napa_250k
databricks bundle run -t dev napa_silver_to_gold_audit --params release_name=napa_5k,analysis_as_of_date=2025-12-31
```

Detailed guide:

```text
docs/silver_to_gold_audit_workflow.md
```

## Inspect Operations Evidence

Shared operations schema:

```text
workspace.instructor_ops
```

Raw-to-Bronze latest runs:

```sql
SELECT *
FROM workspace.instructor_ops.pipeline_runs
WHERE pipeline_name = 'raw_to_bronze'
ORDER BY started_ts DESC;
```

Bronze-to-Silver latest runs:

```sql
SELECT *
FROM workspace.instructor_ops.b2s_pipeline_runs
ORDER BY started_ts DESC;
```

Silver-to-Gold latest runs:

```sql
SELECT *
FROM workspace.instructor_ops.pipeline_runs
WHERE pipeline_name = 'silver_to_gold'
ORDER BY started_ts DESC;
```

Gold audit profile:

```sql
SELECT *
FROM workspace.instructor_ops.gold_table_profile_results
WHERE release_name = 'napa_5k'
ORDER BY profiled_ts DESC, build_order;
```

Gold audit anomalies:

```sql
SELECT *
FROM workspace.instructor_ops.gold_quality_results
WHERE release_name = 'napa_5k'
  AND status <> 'PASSED'
ORDER BY evaluated_ts DESC, target_table, severity;
```

Gold audit reconciliation failures:

```sql
SELECT *
FROM workspace.instructor_ops.gold_reconciliation_results
WHERE release_name = 'napa_5k'
  AND status <> 'PASSED'
ORDER BY evaluated_ts DESC, reconciliation_name;
```

## Inspect Published Schemas

For 5K:

```sql
SHOW TABLES IN workspace.instructor_5k_bronze;
SHOW TABLES IN workspace.instructor_5k_silver;
SHOW TABLES IN workspace.instructor_5k_gold;
```

Repeat with `instructor_50k_*` and `instructor_250k_*` for larger releases.

## Failure Handling

1. Identify the failed workflow task.
2. Review the Databricks task output and traceback.
3. Query the relevant operations run table for the run ID.
4. Query table-run, quality, and reconciliation outputs for failed records.
5. Correct configuration, code, or data placement.
6. Redeploy the bundle when code or workflow YAML changes.
7. Rerun the full affected workflow for the selected release.

Do not manually patch published Bronze, Silver, or Gold business tables. The implemented pipelines are designed for deterministic full-refresh reruns.

## Documentation References

- Layer catalog: [implemented_layer_catalog.md](implemented_layer_catalog.md)
- Raw workflow: [raw_to_bronze_workflow.md](raw_to_bronze_workflow.md)
- Bronze-to-Silver workflow: [bronze_to_silver_workflow.md](bronze_to_silver_workflow.md)
- Silver-to-Gold workflow: [silver_to_gold_workflow.md](silver_to_gold_workflow.md)
- Gold audit workflow: [silver_to_gold_audit_workflow.md](silver_to_gold_audit_workflow.md)
- Gold table contracts: [gold_target_schema_registry.md](gold_target_schema_registry.md)
