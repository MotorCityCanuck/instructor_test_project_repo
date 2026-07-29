# Raw Certification Workflow

**Purpose:** This guide explains the instructor-only Databricks workflow used to certify Raw Parquet student-data releases before they are used for downstream engineering validation or student distribution.

## Workflow Summary

The Raw certification workflow is defined in `config/certification/workflows/napa_raw_certification.job.yml`.

It is a single parameterized job that supports all configured release scales:

```text
5k
50k
250k
```

Those aliases normalize to the canonical release names:

```text
napa_5k
napa_50k
napa_250k
```

The job parameters are:

```text
release_type
analysis_as_of_date
config_path
certification_run_id
source_snapshot_path
baseline_id
fail_on
```

Parameter notes:

- `release_type` selects the release profile and is required in practice even though the job has a default.
- `analysis_as_of_date` is optional and freezes lifecycle and recency logic.
- `config_path` is optional and overrides `config/certification/`.
- `certification_run_id` is optional and allows orchestration to supply a durable identifier.
- `source_snapshot_path` is optional and provides reconciliation evidence from the upstream export or PostgreSQL certification path.
- `baseline_id` is optional and is currently interpreted as a snapshot reference for prior-release regression comparison.
- `fail_on` controls the final gate severity. The default is `blocker`.

## Task Shape

The workflow uses three Python script tasks:

```text
resolve_configuration
run_certification
release_gate
```

They execute:

```text
34_rc_resolve_configuration.py
35_rc_run_certification.py
36_rc_release_gate.py
```

Task responsibilities:

- `resolve_configuration`
  - normalizes the release alias;
  - loads certification config;
  - resolves Databricks object names;
  - emits the durable certification run id and shared task values.
- `run_certification`
  - validates the Databricks environment;
  - runs the Phase 1-6 rule engine;
  - builds the Phase 7 assessment;
  - publishes JSON, Markdown, and CSV artifacts;
  - persists durable Delta evidence tables.
- `release_gate`
  - reads the published certification decision and severity counts from task values;
  - fails the job only after artifacts have been written when the release should be blocked.

## Certification Outputs

Each completed run publishes:

- a JSON snapshot;
- a Markdown certification report;
- a CSV finding extract;
- durable Delta records in the operations schema.

Default artifact layout:

```text
/Volumes/<catalog>/<operations_schema>/certification_artifacts/raw_certification/<release_name>/<artifact_created_at_utc>/<certification_run_id>/certification.json
/Volumes/<catalog>/<operations_schema>/certification_artifacts/raw_certification/<release_name>/<artifact_created_at_utc>/<certification_run_id>/certification_report.md
/Volumes/<catalog>/<operations_schema>/certification_artifacts/raw_certification/<release_name>/<artifact_created_at_utc>/<certification_run_id>/findings.csv
```

The timestamp folder uses the certification completion time in UTC with the format `YYYYMMDDTHHMMSSZ`.

Persistence tables:

- `raw_certification_runs`
- `raw_certification_rule_runs`
- `raw_certification_metrics`
- `raw_certification_findings`
- `raw_certification_artifacts`
- `raw_certification_baselines`

These tables are written to the configured shared operations schema, which currently resolves to `workspace.instructor_ops` in the default development profile.

## Decision Model

The workflow produces one of four decisions:

```text
CERTIFIED
CERTIFIED_WITH_WARNINGS
REJECTED
EXECUTION_FAILED
```

Interpretation:

- `CERTIFIED`
  - no blocker or error findings;
  - the release is approved for the intended use.
- `CERTIFIED_WITH_WARNINGS`
  - no blocker or error findings;
  - warning findings are accepted and documented.
- `REJECTED`
  - one or more blocker or error findings prevent release approval.
- `EXECUTION_FAILED`
  - the framework could not complete certification reliably.

The intended-use labels currently map from release role as follows:

```text
development -> CERTIFIED_FOR_DEVELOPMENT
validation -> CERTIFIED_FOR_ENGINEERING_VALIDATION
production -> CERTIFIED_FOR_PRODUCTION_ANALYTICS
```

The score is diagnostic only. The final gate is decision-based and may also be tightened with `fail_on=warning`.

## Release Gate Behavior

The `release_gate` task behaves as follows:

- it always allows `CERTIFIED`;
- it allows `CERTIFIED_WITH_WARNINGS` by default;
- it blocks `REJECTED`;
- it blocks `EXECUTION_FAILED`;
- it blocks `CERTIFIED_WITH_WARNINGS` when `fail_on=warning`;
- it can be disabled with `fail_on=never`.

This design ensures the report remains available after a rejected certification run.

## Example Commands

Bundle validation and deploy:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Run examples:

```bash
databricks bundle run -t dev napa_raw_certification --params release_type=5k
databricks bundle run -t dev napa_raw_certification --params release_type=50k,analysis_as_of_date=2026-06-30
databricks bundle run -t dev napa_raw_certification --params release_type=250k,analysis_as_of_date=2026-06-30,fail_on=warning
```

Direct Python examples from the spec-equivalent interface:

```bash
python -m napa_pipeline.certification.raw --release-name 5k --analysis-as-of-date 2026-06-30
python -m napa_pipeline.certification.raw --release-name 50k --analysis-as-of-date 2026-06-30
python -m napa_pipeline.certification.raw --release-name 250k --analysis-as-of-date 2026-06-30
```

## Recommended Calibration Sequence

The repository now includes calibration metadata in `config/certification/calibration.yml`, but live calibration must still be executed in Databricks.

Required calibration cases:

- the known 90%-inactive release;
- a release with correct active population;
- a release with broken team identities;
- a release with insufficient Canada mixed-team depth;
- a release with source-to-Raw row loss;
- a full valid `250k` release.

Threshold metadata is currently marked as:

- `provisional` for active gating and pathway-readiness thresholds;
- `observational` for softer comparison and confidence thresholds.

Do not mark thresholds as `approved` until the live calibration cases have been executed and reviewed.

## Production Release Procedure

1. Resolve the target release and confirm the canonical Raw path.
2. Deploy the latest bundle revision.
3. Run `napa_raw_certification` for the target release.
4. Review the Markdown report, JSON snapshot, and CSV findings artifact.
5. Confirm the decision is acceptable for the intended use.
6. Record the certification run id, artifact paths, and accepted exceptions.
7. Update the approved baseline reference when the release should become a comparison anchor.

## Rollback and Recertification Procedure

1. Stop the student-data release and preserve the failing certification artifacts.
2. Restore the last approved upstream release or export snapshot.
3. Record the rollback reason against the failing certification run id.
4. Remediate the upstream export or data-generation issue.
5. Rerun `napa_raw_certification` against the corrected release.
6. Approve recertification only after reviewing the new artifacts and gate decision.

## Troubleshooting

Common failure patterns:

- `RAW_PATH_EXISTS`
  - The configured raw volume path does not exist or is not accessible.
- `RAW_REQUIRED_FILES_PRESENT`
  - One or more required Parquet domains are missing.
- `RAW_PARQUET_READABLE`
  - Spark cannot read a required file.
- `RAW_MATCH_*` or relationship failures
  - Structural integrity defects exist in the raw release.
- `REJECTED` with published report paths
  - The job failed correctly after publishing artifacts; inspect the report rather than rerunning immediately.

Useful follow-up:

- query `workspace.instructor_ops.raw_certification_runs` for run-level status;
- query `workspace.instructor_ops.raw_certification_findings` for blocker and warning findings;
- inspect the published Markdown report before changing thresholds.
