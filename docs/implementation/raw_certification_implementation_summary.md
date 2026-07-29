# Raw Certification Implementation Summary

## Purpose

Summarize the implemented Databricks Raw certification module, its workflow resources, validation coverage, and the remaining live-calibration work required before formal instructor release-gate adoption.

## Files Added

Configuration and workflow:

- `config/certification/base.yml`
- `config/certification/calibration.yml`
- `config/certification/environments/napa_5k.yml`
- `config/certification/environments/napa_50k.yml`
- `config/certification/environments/napa_250k.yml`
- `config/certification/raw_schema.yml`
- `config/certification/workflows/napa_raw_certification.job.yml`

Source module:

- `src/napa_pipeline/certification/__init__.py`
- `src/napa_pipeline/certification/assessment.py`
- `src/napa_pipeline/certification/assignment_probes.py`
- `src/napa_pipeline/certification/cli.py`
- `src/napa_pipeline/certification/config.py`
- `src/napa_pipeline/certification/environment.py`
- `src/napa_pipeline/certification/fitness_rules.py`
- `src/napa_pipeline/certification/models.py`
- `src/napa_pipeline/certification/persistence.py`
- `src/napa_pipeline/certification/reconciliation.py`
- `src/napa_pipeline/certification/reporting.py`
- `src/napa_pipeline/certification/source_loader.py`
- `src/napa_pipeline/certification/structural_rules.py`
- `src/napa_pipeline/certification/workflow.py`

Databricks Python task entrypoints:

- `notebooks/34_rc_resolve_configuration.py`
- `notebooks/35_rc_run_certification.py`
- `notebooks/36_rc_release_gate.py`

Documentation:

- `docs/raw_certification_workflow.md`
- `docs/implementation/raw_certification_repository_assessment.md`
- `docs/implementation/raw_certification_implementation_summary.md`

Tests:

- `tests/test_certification_assessment.py`
- `tests/test_certification_assignment_probes.py`
- `tests/test_certification_cli.py`
- `tests/test_certification_config.py`
- `tests/test_certification_environment.py`
- `tests/test_certification_fitness_rules.py`
- `tests/test_certification_persistence.py`
- `tests/test_certification_reconciliation.py`
- `tests/test_certification_reporting.py`
- `tests/test_certification_source_loader.py`
- `tests/test_certification_structural_rules.py`
- `tests/test_certification_workflow.py`
- `tests/test_certification_workflow_definition.py`

## Files Modified

- `databricks.yml`
- `docs/README.md`

## Architectural Decisions

- Implemented Raw certification as a first-class top-level module under `src/napa_pipeline/certification/` rather than embedding it in `raw_to_bronze`.
- Reused repository conventions already established by `raw_to_bronze`, `bronze_to_silver`, and `silver_to_gold`:
  - layered YAML configuration;
  - Databricks Python script tasks;
  - shared operations-schema persistence pattern;
  - unit-test-first implementation;
  - deterministic artifact generation.
- Kept one Databricks workflow for all scales. Only configuration, paths, and thresholds vary by release.
- Separated publication from gating so rejected releases still publish JSON, Markdown, CSV, and Delta evidence before the workflow fails.
- Kept calibration metadata configuration-driven in `config/certification/calibration.yml` instead of hard-coding threshold maturity in code.

## Rule Count by Pillar

- Release Inventory and Format: 6 inventory and manifest rules.
- Schema and Structural Integrity: 18 source-schema, key, relationship, and match-structure rules across configured domains.
- Population and Lifecycle Fitness: 2 primary rules.
- Team and Partnership Fitness: 1 primary rule.
- Competition and Evidence Fitness: 2 primary rules.
- Ratings, Confidence, and Development Fitness: 3 primary rules.
- Assignment Pathway Readiness: 7 probe rules.
- Source Reconciliation and Regression: 8 reconciliation and comparison rules.

Total implemented rule families: 47 rule outcomes are covered by the current local unit suite.

## Parameters

Workflow and harness parameters:

```text
release_type
analysis_as_of_date
config_path
certification_run_id
source_snapshot_path
baseline_id
fail_on
```

## Workflow Name

Databricks Asset Bundle job:

```text
napa_raw_certification
```

Task keys:

```text
resolve_configuration
run_certification
release_gate
```

## Audit Tables

The certification persistence model writes to the configured operations schema:

```text
raw_certification_runs
raw_certification_rule_runs
raw_certification_metrics
raw_certification_findings
raw_certification_artifacts
raw_certification_baselines
```

Under the default development profile these resolve to:

```text
workspace.instructor_ops.raw_certification_runs
workspace.instructor_ops.raw_certification_rule_runs
workspace.instructor_ops.raw_certification_metrics
workspace.instructor_ops.raw_certification_findings
workspace.instructor_ops.raw_certification_artifacts
workspace.instructor_ops.raw_certification_baselines
```

## Report Locations

Published artifacts are written under the configured artifacts root:

```text
/Volumes/<catalog>/<operations_schema>/certification_artifacts/raw_certification/<release_name>/<artifact_created_at_utc>/<certification_run_id>/certification.json
/Volumes/<catalog>/<operations_schema>/certification_artifacts/raw_certification/<release_name>/<artifact_created_at_utc>/<certification_run_id>/certification_report.md
/Volumes/<catalog>/<operations_schema>/certification_artifacts/raw_certification/<release_name>/<artifact_created_at_utc>/<certification_run_id>/findings.csv
```

## Tests Added

Key coverage areas:

- alias normalization and CLI parsing;
- config merge/validation;
- environment resolution;
- inventory and manifest checks;
- structural and relationship rules;
- fitness rules;
- assignment-pathway probes;
- reconciliation/regression rules;
- assessment and decision model;
- persistence record generation;
- deterministic report and snapshot rendering;
- workflow release-gate logic;
- Databricks workflow definition structure.

## Test Results

Most recent local validation:

```text
python -m pytest tests\test_certification_cli.py tests\test_certification_config.py tests\test_certification_environment.py tests\test_certification_source_loader.py tests\test_certification_structural_rules.py tests\test_certification_fitness_rules.py tests\test_certification_assignment_probes.py tests\test_certification_reconciliation.py tests\test_certification_assessment.py tests\test_certification_persistence.py tests\test_certification_reporting.py tests\test_certification_workflow.py tests\test_certification_workflow_definition.py -q
57 passed in 3.17s
```

Compile validation:

```text
python -m py_compile src\napa_pipeline\certification\assessment.py src\napa_pipeline\certification\persistence.py src\napa_pipeline\certification\reporting.py src\napa_pipeline\certification\workflow.py src\napa_pipeline\certification\cli.py src\napa_pipeline\certification\__init__.py notebooks\34_rc_resolve_configuration.py notebooks\35_rc_run_certification.py notebooks\36_rc_release_gate.py
```

Diff validation:

```text
git diff --check
```

## Performance Observations

- Local unit-test runtime for the certification module remained under four seconds on the current developer workstation.
- No acceptance runtime has been hard-coded for Databricks execution.
- The `250k` performance requirement still requires a live workspace run to record total runtime, Spark stages, shuffle volume, and any driver-memory pressure.

## Deviations From The Specification

- `baseline_id` is currently interpreted as a snapshot reference or path for prior-release regression input. It is not yet resolved through a baseline catalog lookup.
- Cross-scale snapshots are supported by the reconciliation module, but the Phase 8 Databricks harness does not yet resolve or inject them automatically.
- The spec’s example CLI entrypoint `python -m napa_pipeline.certification.raw` is documented but not yet implemented as a separate local CLI module in this repository.
- Threshold maturity is recorded in configuration, but thresholds remain provisional or observational until live Databricks calibration cases are executed.

## Unresolved Risks

- Live Databricks calibration has not yet been executed against the required known-good and known-bad releases.
- Approved baselines are still placeholders pending live certification evidence.
- The final release procedure depends on upstream export snapshots being consistently available for reconciliation.
- The `250k` end-to-end certification run has not yet been observed in the target Databricks environment for performance validation.

## Commands To Run 5K, 50K, and 250K

Bundle validation and deploy:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Run 5K:

```bash
databricks bundle run -t dev napa_raw_certification --params release_type=5k,analysis_as_of_date=2026-06-30
```

Run 50K:

```bash
databricks bundle run -t dev napa_raw_certification --params release_type=50k,analysis_as_of_date=2026-06-30
```

Run 250K:

```bash
databricks bundle run -t dev napa_raw_certification --params release_type=250k,analysis_as_of_date=2026-06-30
```

Optional stricter gate:

```bash
databricks bundle run -t dev napa_raw_certification --params release_type=250k,analysis_as_of_date=2026-06-30,fail_on=warning
```
