# Silver-to-Gold Databricks Workflow

**Purpose:** This guide explains the Silver-to-Gold Databricks Workflow resource, its task graph, bundle wiring, and the current execution boundary in this repository.

## Workflow Summary

The workflow is defined in `config/silver_to_gold/workflows/napa_silver_to_gold.job.yml`.

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

`analysis_as_of_date` is optional. Leave it empty to let the Gold runtime resolve the maximum valid match date from the deployed Silver match data.

## Task Order

The job runs these Python script tasks in order:

```text
build_competition_foundation
        |
build_persistent_team_resolution
        |
build_analytical_ratings
        |
build_player_features
        |
build_team_features
        |
build_quality_confidence
        |
build_match_outcome_products
        |
build_scorecards_and_rankings
        |
build_olympic_candidates
        |
build_olympic_recommendations
        |
build_sensitivity_and_explanations
```

## Deployment

The root bundle includes all implemented workflow directories:

- `config/raw_to_bronze/workflows/*.yml`
- `config/bronze_to_silver/workflows/*.yml`
- `config/silver_to_gold/workflows/*.yml`

The Silver-to-Gold workflow directory currently defines both:

- `napa_silver_to_gold`
- `napa_silver_to_gold_audit`

The root bundle sync now includes:

- `config/raw_to_bronze/**`
- `config/bronze_to_silver/**`
- `config/silver_to_gold/**`
- `notebooks/**`
- `src/**`
- `requirements.txt`

For Databricks Free Edition, the Silver-to-Gold job uses serverless compute. The workflow defines:

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

## Run Commands

Examples:

```bash
databricks bundle run -t dev napa_silver_to_gold --params release_name=napa_5k
databricks bundle run -t dev napa_silver_to_gold --params release_name=napa_50k
databricks bundle run -t dev napa_silver_to_gold --params release_name=napa_250k
databricks bundle run -t dev napa_silver_to_gold --params release_name=napa_5k,analysis_as_of_date=2025-12-31
```

## Current Execution Boundary

This workflow resource currently orchestrates the validated Phase 3 through Phase 13 Gold script harnesses:

- `21_g2g_phase3_competition_foundation_harness.py`
- `22_g2g_phase4_persistent_team_resolution_harness.py`
- `24_g2g_phase5_analytical_rating_engine_harness.py`
- `25_g2g_phase6_player_feature_harness.py`
- `26_g2g_phase7_team_feature_harness.py`
- `27_g2g_phase8_entity_quality_confidence_harness.py`
- `28_g2g_phase9_match_outcome_harness.py`
- `29_g2g_phase10_player_scorecard_harness.py`
- `30_g2g_phase11_team_selection_harness.py`
- `31_g2g_phase12_recommendation_harness.py`
- `32_g2g_phase13_sensitivity_harness.py`

That means the workflow is deployment-ready and launchable, but it still reflects the current phase-harness execution boundary:

- each task is a fully validated standalone Gold phase entrypoint;
- each phase task performs its own source-contract validation and publication;
- each phase task currently writes its own Gold pipeline run record in `workspace.instructor_ops`.

So the workflow is accurate as an orchestration layer for the existing instructor reference build, but it is not yet the final unified single-run Gold orchestration model. A later refinement can add workflow-level resolve/finalize tasks if a single shared Gold run identity becomes required.
