# Runbook

**Purpose:** This document provides step-by-step instructions for setting up the project, configuring the active dataset, running notebooks, validating outputs, and preparing milestone submissions for the NAPA Olympic Analytics Platform. The template provides a reproducibility structure only; teams must document their own environment-specific steps, assumptions, and troubleshooting notes.

## Prerequisites

- List required accounts, tools, and local setup items.
- Note which items are optional versus required.

## Clone Repository

- Document the repository clone process used by the team.
- Record any template-specific setup actions after cloning.

## Create Local Environment

- Document how the team creates and activates a local environment.
- Record any approved package additions beyond the starter requirements.

## Configure Dataset

- Document how the active dataset is selected.
- Record local versus Databricks path differences.
- Note how local configuration files are managed safely.

## Databricks Setup Notes

- Record workspace assumptions, catalog/schema setup, and storage decisions.
- Databricks Free Edition is serverless-only. Do not document or depend on existing clusters, job clusters, or cluster IDs for this repository's workflows.
- Note any other differences between Databricks Free Edition and other environments.

## Run Notebook Sequence

- Document the intended notebook execution order.
- Record prerequisites, expected outputs, and checkpoints for each notebook.

## Raw-to-Bronze Workflow

The instructor Raw-to-Bronze pipeline is operated as a Databricks Workflow with one user-supplied parameter, `release_type`.

Bundle commands:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev napa_raw_to_bronze --params release_type=5k
databricks bundle run -t dev napa_raw_to_bronze --params release_type=50k
databricks bundle run -t dev napa_raw_to_bronze --params release_type=250k
```

UI run path:

1. Deploy the bundle with `databricks bundle deploy -t dev`.
2. Open **Workflows** in the Databricks workspace UI.
3. Open `NAPA Raw to Bronze`.
4. Click **Run now**.
5. Enter `release_type` as `5k`, `50k`, or `250k`.
6. Start the run.

Use [`raw_to_bronze_workflow.md`](raw_to_bronze_workflow.md) for deployment, execution, failure inspection, and rerun procedures.

## Review Outputs

- Describe which outputs should be reviewed after each run.
- Note where quality findings, scorecards, and figures are stored.

## Prepare Milestone Evidence

- Document which artifacts belong in each milestone folder.
- Record how commits and milestone summaries support review evidence.

## Troubleshooting

- Capture recurring setup or execution issues.
- Record how the team resolved them.

## Known Environment Differences

- Note any differences across local development, Databricks, and reviewer environments.
