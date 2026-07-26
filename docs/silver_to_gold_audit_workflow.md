# Silver-to-Gold Audit Workflow

**Purpose:** This guide explains the standalone Databricks job used to describe and validate the published Gold layer after the main Silver-to-Gold workflow completes.

## Workflow Summary

The standalone audit workflow is defined in `config/silver_to_gold/workflows/napa_silver_to_gold_audit.job.yml`.

It exposes the same runtime parameters as the main Gold workflow:

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

`analysis_as_of_date` is optional. Leave it empty to let the audit runtime resolve the maximum valid match date from the deployed Silver match data. Pass an explicit date when you need to validate a specific historical Gold build.

## Task Shape

The standalone job runs one Python script task:

```text
audit_gold_layer
```

That task executes:

```text
33_g2g_gold_layer_audit_harness.py
```

## What The Audit Does

The audit reads the materialized Gold outputs from the current workflow boundary:

- `competition_match_sides`
- `competition_player_matches`
- `resolved_match_teams`
- `player_rating_events`
- `player_rating_history`
- `player_current_ratings`
- `player_performance_features`
- `player_development_features`
- `team_performance_features`
- `partnership_effectiveness`
- `entity_data_quality_confidence`
- `match_outcome_training_set`
- `match_outcome_predictions`
- `match_model_metrics`
- `player_evaluation_scorecards`
- `national_player_rankings`
- `team_selection_scorecards`
- `olympic_team_candidates`
- `olympic_team_recommendations`
- `selection_sensitivity_results`
- `recommendation_explanations`

For each published Gold table, the audit:

- confirms the table exists;
- records row count and column count;
- validates null-free primary keys;
- validates primary-key uniqueness;
- profiles null rates and approximate distinct counts for every column;
- checks `analysis_as_of_date` alignment where the column exists;
- checks `scoring_scenario` alignment where the column exists.

It also runs explicit cross-table reconciliation checks:

- `competition_player_matches = competition_match_sides * 2`
- `player_performance_features = player_development_features * performance_window_count`
- `recommendation_explanations = olympic_team_recommendations`
- `selection_sensitivity_results = olympic_team_recommendations * configured_sensitivity_scenarios`

## Audit Outputs

The job writes audit metadata into the shared Gold operations schema:

- `gold_table_profile_results`
- `gold_column_profile_results`
- `gold_quality_results`
- `gold_reconciliation_results`

Interpretation:

- `gold_table_profile_results` contains one row per audited Gold table.
- `gold_column_profile_results` contains one row per audited table-column pair.
- `gold_quality_results` contains explicit anomaly records such as missing tables, null primary keys, duplicate keys, and alignment failures.
- `gold_reconciliation_results` contains explicit cross-table row-balance checks.

The audit job fails when it detects critical structural anomalies. Warnings, such as an empty table, are recorded but do not fail the run by themselves.

Failed audit runs are recorded in `pipeline_runs` with `processing_mode = 'gold_audit'`. The harness persists the table profiles, column profiles, quality results, and reconciliation results before it fails the task for critical anomalies. The raised error includes a compact list of failed quality and reconciliation checks, and the full details remain available in `gold_quality_results` and `gold_reconciliation_results`.

Useful triage queries:

```sql
SELECT
    target_table,
    rule_id,
    severity,
    failed_row_count,
    failure_pct,
    sample_keys
FROM workspace.instructor_ops.gold_quality_results
WHERE pipeline_run_id = '<gold_audit_pipeline_run_id>'
  AND status = 'FAILED'
ORDER BY severity DESC, target_table, rule_id;
```

```sql
SELECT
    reconciliation_name,
    source_count,
    accepted_count,
    difference,
    status
FROM workspace.instructor_ops.gold_reconciliation_results
WHERE pipeline_run_id = '<gold_audit_pipeline_run_id>'
  AND status = 'FAILED'
ORDER BY reconciliation_name;
```

## Deployment

The root bundle already includes `config/silver_to_gold/workflows/*.yml`, so this workflow is picked up automatically by:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Run Commands

Examples:

```bash
databricks bundle run -t dev napa_silver_to_gold_audit --params release_name=napa_5k
databricks bundle run -t dev napa_silver_to_gold_audit --params release_name=napa_50k
databricks bundle run -t dev napa_silver_to_gold_audit --params release_name=napa_250k
databricks bundle run -t dev napa_silver_to_gold_audit --params release_name=napa_5k,analysis_as_of_date=2025-12-31
```

## Recommended Usage

Run this workflow only after the main `napa_silver_to_gold` workflow completes successfully for the same `release_name` and, when used, the same `analysis_as_of_date`.

Suggested progression:

1. validate `napa_5k`;
2. inspect the audit outputs and anomaly records;
3. rerun after any Gold fixes;
4. then scale to `napa_50k`;
5. then scale to `napa_250k`.
