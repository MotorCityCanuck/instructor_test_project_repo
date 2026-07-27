# Raw Certification Repository Assessment

## Purpose

Assess the current repository against the requirements in `docs/NAPA_Databricks_Raw_Student_Data_Certification_Specification_and_Implementation_Plan.md` and define the implementation approach for a Databricks-native Raw certification module.

## Scope

This assessment covers:

- current repository patterns relevant to certification;
- reusable components already present in the repo;
- missing components required by the specification;
- files that will need to be added or modified;
- implementation sequencing;
- risks and open questions to resolve before or during build-out.

This document does not implement the certification module itself.

## Current Repository State

The repository already contains working Databricks pipeline patterns for:

- `raw_to_bronze`
- `bronze_to_silver`
- `silver_to_gold`
- standalone audit workflows for Silver and Gold

These existing modules establish the conventions the Raw certification module should follow rather than introducing a separate framework.

## Relevant Existing Assets

### Bundle and workflow conventions

- `databricks.yml`
  - currently includes workflow YAML only for `raw_to_bronze`, `bronze_to_silver`, and `silver_to_gold`
  - currently syncs only those config trees plus `notebooks/**`, `src/**`, and `requirements.txt`
- `config/raw_to_bronze/workflows/napa_raw_to_bronze.job.yml`
  - reference pattern for Databricks Asset Bundle job structure

### Configuration and release normalization

- `src/napa_pipeline/raw_to_bronze/config.py`
  - reference implementation for layered YAML config loading
- `src/napa_pipeline/raw_to_bronze/cli.py`
  - reference implementation for Databricks script-task argument parsing
  - currently accepts short release aliases only: `5k`, `50k`, `250k`
- `config/raw_to_bronze/base.yml`
  - reference for pipeline metadata, runtime, execution policy, and publication settings

### Environment resolution

- `src/napa_pipeline/raw_to_bronze/environment.py`
  - resolves catalog, schemas, and managed objects from config
  - establishes the existing style for Databricks environment validation

### Raw-file inventory and readiness checks

- `src/napa_pipeline/raw_to_bronze/inventory.py`
  - validates raw file presence against expected source inventory
  - validates file readability and row-count/schema readiness from Parquet
  - closest existing technical foundation for Raw certification checks

### Durable operational persistence

- `src/napa_pipeline/raw_to_bronze/operations.py`
- `src/napa_pipeline/bronze_to_silver/operations.py`
- `src/napa_pipeline/silver_to_gold/operations.py`
  - establish the repo pattern for persistent run metadata and per-step results in Delta tables

### Audit and reporting patterns

- `src/napa_pipeline/bronze_to_silver/silver_audit.py`
- `src/napa_pipeline/silver_to_gold/gold_audit.py`
  - reference patterns for rule execution, summarized results, and persisted audit outputs

### Existing tests that can guide implementation

- `tests/test_raw_to_bronze_cli.py`
- `tests/test_raw_to_bronze_inventory.py`
- `tests/test_silver_to_gold_audit.py`
- `tests/test_silver_to_gold_audit_workflow_definition.py`

## Repository-to-Spec Fit

The specification aligns well with the repository’s current engineering style.

Strong alignment areas:

- configuration-driven pipelines;
- Databricks Asset Bundle workflows;
- notebook-orchestrated Python script tasks;
- persistent Delta-based operational tracking;
- release-scoped environment resolution;
- test-first module structure under `src/` and `tests/`.

The certification module should therefore be implemented as a new top-level pipeline area, not folded into `raw_to_bronze`.

Recommended module root:

- `src/napa_pipeline/certification/`

Recommended workflow/config root:

- `config/certification/`

## Gaps Relative to the Specification

The following required capabilities do not yet exist in the repository as dedicated certification components.

### 1. Certification pipeline module

Missing:

- certification-specific config loader;
- certification CLI/parser;
- certification environment resolver;
- certification rule registry and execution engine;
- certification findings model;
- certification artifact writer;
- certification workflow driver;
- certification summary/fail-gate logic.

### 2. Certification workflow bundle definition

Missing:

- DAB workflow YAML for Raw certification
- bundle include/sync entries for the certification config tree

### 3. Certification operations tables

Missing:

- dedicated Delta tables for certification runs, rule results, findings, and artifacts

Existing operations modules are useful references, but the specification requires certification-specific result structures and artifact traceability.

### 4. Artifact generation

Missing:

- persisted JSON summary artifact
- persisted Markdown summary artifact
- persisted CSV findings artifact
- artifact metadata publication to ops tables

### 5. Rule implementation

Missing:

- schema contract checks
- file inventory checks
- row-count and emptiness checks
- duplicate key checks where defined
- enumerated value and structural contract checks
- release alias normalization per certification spec
- certification-blocking severity model

### 6. Tests for the certification module

Missing:

- config tests
- CLI normalization tests
- rule execution tests
- artifact writing tests
- workflow-definition tests

## Expected File Changes

### New files expected

Configuration and workflow:

- `config/certification/base.yml`
- `config/certification/releases/*.yml`
- `config/certification/environments/*.yml`
- `config/certification/workflows/napa_raw_certification.job.yml`
- `config/certification/rules/*.yml` or equivalent rule registry file

Source module:

- `src/napa_pipeline/certification/__init__.py`
- `src/napa_pipeline/certification/config.py`
- `src/napa_pipeline/certification/cli.py`
- `src/napa_pipeline/certification/environment.py`
- `src/napa_pipeline/certification/models.py`
- `src/napa_pipeline/certification/rules.py`
- `src/napa_pipeline/certification/persistence.py`
- `src/napa_pipeline/certification/artifacts.py`
- `src/napa_pipeline/certification/workflow.py`
- `src/napa_pipeline/certification/summary.py`

Notebook/script-task entrypoints:

- `notebooks/31_raw_cert_01_resolve_configuration.py`
- `notebooks/32_raw_cert_02_prepare_environment.py`
- `notebooks/33_raw_cert_03_execute_rules.py`
- `notebooks/34_raw_cert_04_publish_artifacts.py`
- `notebooks/35_raw_cert_05_finalize_run.py`

Tests:

- `tests/test_certification_cli.py`
- `tests/test_certification_config.py`
- `tests/test_certification_rules.py`
- `tests/test_certification_artifacts.py`
- `tests/test_certification_workflow_definition.py`

Documentation:

- `docs/raw_certification_workflow.md`

### Existing files expected to change

- `databricks.yml`
  - add `config/certification/workflows/*.yml` to `include`
  - add `config/certification/**` to `sync.include`

Possibly:

- `requirements.txt`
  - only if artifact-generation or serialization requirements are not already satisfied by the current dependency set

## Recommended Design Decisions

### 1. Keep certification independent of Raw-to-Bronze

Do not embed certification logic in `raw_to_bronze`.

Reason:

- the spec defines certification as a separate control point against Raw Parquet exports;
- separation preserves clarity between ingestion and release certification;
- the repo already uses dedicated top-level module areas by pipeline.

### 2. Reuse the Raw inventory validation style

Base the file-discovery and readability checks on the existing `raw_to_bronze.inventory` approach, but keep certification-specific rule output and messaging in the new certification module.

### 3. Broaden release alias normalization

Certification config/CLI should accept:

- `5k`
- `50k`
- `250k`
- `napa_5k`
- `napa_50k`
- `napa_250k`

Normalization should resolve to a canonical release name for downstream environment/config lookup.

### 4. Use dedicated certification ops tables

Do not overload existing raw-to-bronze operational tables.

The certification workflow needs durable rule-level and artifact-level outputs that are semantically distinct from Bronze publication runs.

### 5. Persist findings before applying the gate

Certification must publish all findings and artifacts before blocking the workflow on critical failures.

This matches the repository’s updated audit philosophy and improves diagnosability in Databricks.

## Proposed Implementation Sequence

### Phase 1: module skeleton and config loading

Build:

- config structure
- config loader
- release normalization
- CLI argument parsing
- environment resolution

Validation:

- unit tests for config and CLI

### Phase 2: rule execution engine

Build:

- certification result models
- file inventory rules
- Parquet readability rules
- schema contract rules
- structural and completeness rules
- severity and gating model

Validation:

- rule unit tests using synthetic fixtures

### Phase 3: persistence and artifacts

Build:

- Delta persistence tables
- JSON/Markdown/CSV artifact generation
- artifact metadata recording

Validation:

- artifact and persistence tests

### Phase 4: Databricks workflow integration

Build:

- notebook/script entrypoints
- DAB workflow YAML
- bundle include/sync updates

Validation:

- workflow definition tests
- dry-run configuration validation

### Phase 5: workflow documentation

Build:

- `docs/raw_certification_workflow.md`

Validation:

- confirm docs reflect actual config, notebook names, and workflow behavior

## Risks and Open Questions

### 1. Raw contract source of truth

Open question:

- should certification schema and rule expectations be driven from a dedicated certification contract file, from `raw_to_bronze` source inventory config, or from both?

Recommendation:

- use a certification-specific contract file that can reference the existing raw source inventory where appropriate

### 2. Ops schema naming

Open question:

- should certification write to the existing release operations schema, or to a dedicated certification schema?

Current repo precedent:

- existing pipelines use a shared release operations schema pattern

Recommendation:

- keep certification outputs in the release operations schema unless a separate schema is explicitly required

### 3. Artifact storage location

Open question:

- should generated JSON/Markdown/CSV artifacts be stored in a UC Volume path, DBFS-style workspace path, or only represented as Delta rows?

Recommendation:

- publish artifacts to a managed Volume path resolved from config and also store artifact metadata in Delta

### 4. Rule scope boundaries

Open question:

- which checks are certification checks versus Bronze readiness checks?

Recommendation:

- certification should remain focused on Raw export integrity and release fitness, not duplicate Bronze transformation logic

## Build Readiness Assessment

The repository is ready to support the Raw certification build.

Reasons:

- existing pipeline structure is mature and consistent;
- Databricks bundle and script-task patterns are already established;
- raw-file validation logic already exists in adjacent form;
- test conventions for config, workflow, and audit modules are present.

Primary implementation constraint:

- the certification module should be introduced as a first-class pipeline area with dedicated config, ops persistence, and workflow definitions, rather than extending an existing pipeline opportunistically.

## Recommended Next Step

Proceed with implementation of the certification module in a new `src/napa_pipeline/certification/` area, starting with config loading, CLI normalization, and environment resolution, then layering rules, persistence, artifacts, and Databricks workflow integration in that order.
