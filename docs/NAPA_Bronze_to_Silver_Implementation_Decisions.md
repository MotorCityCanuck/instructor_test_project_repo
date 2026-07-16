# NAPA Bronze-to-Silver Implementation Decisions

**Purpose:** This note freezes the initial implementation shape for the instructor Bronze-to-Silver build so configuration, code, workflow, and testing can proceed without re-deciding foundational repo structure on each change.

**Audience:** Instructor, implementation reviewer, AI coding assistant

**Status:** Active implementation baseline

---

## Decision Summary

The Bronze-to-Silver build will follow the existing repository and Raw-to-Bronze implementation patterns unless a later technical constraint forces a documented change.

### 1. Python package root

Keep the existing package root:

```text
src/napa_pipeline
```

Do not introduce a parallel `src/pipeline` package.

Reason:

- the repository already has an established Python package;
- the Raw-to-Bronze implementation already uses `src/napa_pipeline/raw_to_bronze`;
- keeping one package root reduces unnecessary import, test, and workflow churn.

### 2. Bronze-to-Silver package layout

Create a dedicated Bronze-to-Silver package under the existing root:

```text
src/napa_pipeline/bronze_to_silver/
```

This package should own:

- configuration loading and validation;
- environment resolution;
- operations logging helpers specific to Silver behavior where needed;
- reusable transformation and validation logic;
- table-build orchestration;
- reconciliation and finalization helpers.

### 3. Configuration layout

Create a dedicated configuration surface parallel to Raw-to-Bronze:

```text
config/bronze_to_silver/
```

Expected files:

```text
base.yml
environments/napa_5k.yml
environments/napa_50k.yml
environments/napa_250k.yml
sources.yml
silver_tables.yml
domains.yml
quality_rules.yml
logging.yml
workflows/
```

Reason:

- this matches the current Raw-to-Bronze structure;
- it keeps Silver configuration isolated from Bronze ingestion configuration;
- it aligns with the Bronze-to-Silver spec without forcing broad repo reorganization.

### 4. Workflow deployment model

Use the same root Databricks Asset Bundle pattern already established for Raw-to-Bronze.

The Bronze-to-Silver workflow definition should live under:

```text
config/bronze_to_silver/workflows/
```

The root [databricks.yml](D:/@Repos/instructor_test_project_repo/databricks.yml) should remain the single bundle entry point for Databricks deployment.

### 5. Databricks compute model

Assume Databricks Free Edition is serverless-only.

Therefore:

- workflow tasks must use serverless job-task configuration;
- do not use `existing_cluster_id`;
- do not use job clusters;
- do not require cluster IDs in config or documentation.

### 6. Workflow parameter shape

Use one primary Bronze-to-Silver workflow parameter:

```text
release_name
```

Allowed values:

```text
napa_5k
napa_50k
napa_250k
```

Reason:

- this matches the Bronze-to-Silver spec;
- it keeps release selection aligned with release-specific configuration files;
- it distinguishes the Silver workflow from the Raw-to-Bronze `release_type` parameter without ambiguity.

### 7. Task entrypoint style

Use Databricks Python script tasks, not notebook widgets.

Thin task entrypoints should remain under:

```text
notebooks/
```

The scripts should:

- parse task arguments;
- bootstrap package imports;
- load resolved configuration;
- call reusable package code;
- publish task values required by downstream tasks.

### 8. Operations model

Keep one shared operations schema and distinguish pipelines by `pipeline_name`.

Bronze-to-Silver should reuse the established operational approach from Raw-to-Bronze where practical, but it may extend the shared pattern for:

- `quality_results`;
- Silver-specific reconciliation details;
- reject-table evidence.

### 9. Student-facing boundary

This implementation remains instructor reference work.

The Bronze-to-Silver code may implement the full engineering pipeline, but it must not expand into Gold-layer analytics such as:

- ranking logic;
- roster recommendations;
- chemistry scoring;
- predictive features;
- simulation outputs.

---

## Immediate Implications

The next implementation step should build the Bronze-to-Silver configuration model inside:

- `config/bronze_to_silver/`
- `src/napa_pipeline/bronze_to_silver/config.py`

Tests for that work should be added before workflow wiring begins.
