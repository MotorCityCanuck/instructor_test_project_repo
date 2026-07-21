# NAPA Bronze-to-Silver Implementation Decisions

**Purpose:** This note freezes the initial implementation shape for the instructor Bronze-to-Silver build and records the Phase 0 repository and Silver-contract verification baseline that downstream Silver-to-Gold work must honor.

**Audience:** Instructor, implementation reviewer, AI coding assistant

**Status:** Active implementation baseline

---

## Decision Summary

The Bronze-to-Silver build will follow the existing repository and Raw-to-Bronze implementation patterns unless a later technical constraint forces a documented change.

## Phase 0 Verification Baseline

Phase 0 verification was completed on July 20, 2026 against the current repository state before any Silver-to-Gold work began.

### Verified repository state

- Current branch: `main`
- Working tree: clean at verification time
- Remote: `origin https://github.com/MotorCityCanuck/instructor_test_project_repo`
- Root documents reviewed: `AGENTS.md`, `README.md`, `docs/README.md`
- Silver implementation documents reviewed:
  - `docs/NAPA_Bronze_to_Silver_Spec.md`
  - `docs/NAPA_Bronze_to_Silver_Plan.md`
  - `docs/NAPA_Bronze_to_Silver_Implementation_Decisions.md`
- Silver implementation surfaces reviewed:
  - `config/bronze_to_silver/`
  - `src/napa_pipeline/bronze_to_silver/`
  - `notebooks/11_b2s_*.py` through `20_b2s_*.py`
  - `tests/test_bronze_to_silver_*.py`
  - `tests/test_raw_to_bronze_to_silver_contract.py`

### Referenced external-document check

The user referenced:

- `napa_silver_to_gold_codex_implementation_plan_v1.0.md`
- `hapa_silver_to_gold_layer_engineering spec_v1.0`

Those files were not present in this repository under `docs/development/`, and they were also not present at `$HOME\\pickleball-sim\\docs\\development` in the current environment. Until those documents are provided in an accessible location, Silver-to-Gold Phase 0 must anchor on the verified repository contract implemented here.

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

## Verified Silver Contract

The current repository already implements a concrete Bronze-to-Silver contract. Silver-to-Gold work should consume this implemented contract rather than re-derive one from a future or external spec.

### 10. Package and configuration authority

The active Bronze-to-Silver implementation authority is:

- package root: `src/napa_pipeline/bronze_to_silver/`
- config root: `config/bronze_to_silver/`
- workflow definition: `config/bronze_to_silver/workflows/napa_bronze_to_silver.job.yml`

For Silver work, YAML configuration is the runtime authority. The older widget helper in `notebooks/_shared_pipeline_config.py` exists, but it should not replace the Bronze-to-Silver config loader for the current instructor pipeline.

### 11. Release and schema convention

The implemented release names are:

- `napa_5k`
- `napa_50k`
- `napa_250k`

The implemented schema convention is release-specific and team-prefixed. For example, `napa_5k` resolves to:

- Bronze schema: `instructor_5k_bronze`
- Silver schema: `instructor_5k_silver`
- Silver reject schema: `instructor_5k_silver_reject`
- Shared operations schema: `instructor_ops`

The runtime catalog currently resolves to `workspace`.

### 12. Silver source contract

The implemented Silver config expects thirteen Bronze sources:

- `monthly_batches`
- `regions`
- `clubs`
- `player_master`
- `teams`
- `player_registrations`
- `player_assessment_history`
- `club_memberships`
- `team_memberships`
- `matches`
- `match_teams`
- `match_team_players`
- `match_games`

The cross-pipeline contract test in `tests/test_raw_to_bronze_to_silver_contract.py` verifies that Bronze-to-Silver consumes the same source inventory, Bronze table names, natural keys, catalog, and Bronze schema as Raw-to-Bronze.

### 13. Silver target contract

The implemented Silver build order contains thirteen enabled targets:

1. `monthly_batches`
2. `regions`
3. `players`
4. `clubs`
5. `teams`
6. `player_registrations`
7. `player_assessment_history`
8. `club_memberships`
9. `team_memberships`
10. `matches`
11. `match_teams`
12. `match_team_players`
13. `match_games`

Each target is configured with:

- one upstream Bronze source;
- a declared build stage;
- a deterministic build order;
- a configured primary key;
- a dedicated reject table name.

### 14. Workflow contract

The current Databricks workflow contract is a single serverless job parameterized by `release_name` and composed of these script tasks:

1. `resolve_configuration`
2. `validate_environment`
3. `validate_bronze_sources`
4. `build_reference`
5. `build_athlete`
6. `build_organization_partnership`
7. `build_competition`
8. `run_cross_table_validation`
9. `publish_convenience_views`
10. `finalize_pipeline_summary`

Downstream tasks consume a shared `run_id` emitted by `resolve_configuration`.

### 15. Gold-boundary verification

The current Gold-layer notebooks remain student-facing outlines:

- `notebooks/04_build_gold_products.py`
- `notebooks/05_executive_analytics.py`

No implemented Gold analytics, rankings, chemistry scoring, roster logic, or executive output logic currently exists in the repository. That boundary remains intact and should remain intact until instructor direction explicitly broadens it.

---

## Immediate Implications

The Phase 0 baseline confirms that this repository is already past initial Bronze-to-Silver scaffolding. Any next step for Silver-to-Gold work should:

- treat the existing Bronze-to-Silver config, workflow, and tests as the authoritative Silver contract;
- avoid renaming package roots, release names, schemas, or task names without a documented reason;
- preserve the current Gold boundary until a Silver-to-Gold implementation spec is available in this environment.
