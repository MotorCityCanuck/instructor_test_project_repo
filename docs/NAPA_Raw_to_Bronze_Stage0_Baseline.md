# NAPA Raw to Bronze Stage 0 Baseline

**Purpose:** This document records the Stage 0 restart baseline for the Raw-to-Bronze rebuild. It establishes which current repository assets are legacy Bronze experimentation, what target architecture will replace them, and what will remain unchanged until the new release-specific pipeline is validated.

**Reference plan:** `docs/NAPA_Raw_to_Bronze_Plan.md`  
**Reference specification:** `docs/NAPA_Raw_to_Bronze_Spec.md`

---

## 1. Current Repo Baseline

At the start of the Raw-to-Bronze rebuild, the repository contains instructor-created Bronze experimentation that does not match the release-specific target architecture defined in the specification.

Observed Bronze-related implementation files:

- `notebooks/01_setup_catalog.py`
- `notebooks/01_ingest_raw_to_bronze.py`
- `notebooks/_shared_pipeline_config.py`
- `src/napa_pipeline/config.py`

Observed current default assumptions inside those files:

- one shared Raw schema such as `instructor_raw`;
- one shared Bronze schema such as `instructor_bronze`;
- one shared dataset path driven by `dataset_name`;
- notebook-scoped execution flow rather than the target Databricks pipeline workflow;
- widget-based configuration rather than the release-specific YAML configuration model in the spec.

These files are useful historical context, but they are not the target implementation pattern for the Raw-to-Bronze rebuild.

---

## 2. Stage 0 Decisions

The following decisions define the restart boundary.

### 2.1 Target Architecture

The rebuild will target the release-specific structure from the Raw-to-Bronze specification:

- `workspace.instructor_5k_raw`
- `workspace.instructor_5k_bronze`
- `workspace.instructor_50k_raw`
- `workspace.instructor_50k_bronze`
- `workspace.instructor_250k_raw`
- `workspace.instructor_250k_bronze`
- `workspace.instructor_ops`

The rebuild will not treat `workspace.instructor_raw` or `workspace.instructor_bronze` as the active architecture.

### 2.2 Legacy Notebook Status

The current Bronze notebooks are classified as legacy instructor experimentation:

- `notebooks/01_setup_catalog.py`
- `notebooks/01_ingest_raw_to_bronze.py`

Stage 0 decision:

- do not extend these files as the primary Raw-to-Bronze implementation;
- do not delete them yet;
- do not use them as the architectural baseline for the rebuild;
- replace their role later with release-specific Databricks workflow tasks and reusable Python modules.

### 2.3 Shared Widget Config Status

The current shared notebook configuration helper and widget-based config module are also legacy for Raw-to-Bronze restart purposes:

- `notebooks/_shared_pipeline_config.py`
- `src/napa_pipeline/config.py`

Stage 0 decision:

- they may remain temporarily for historical context or unrelated work;
- the Raw-to-Bronze rebuild will move to release-specific YAML configuration under `config/raw_to_bronze/`;
- no new Raw-to-Bronze code should depend on the current widget contract as the long-term configuration authority.

### 2.4 Current Databricks Objects

Current Databricks objects created under the earlier instructor schema convention must be retained until the new release-specific pipeline passes acceptance.

Stage 0 decision:

- do not delete existing Raw, Bronze, Silver, Gold, or Ops objects during the restart;
- use the new pipeline to build separate release-specific namespaces;
- compare new outputs to existing instructor objects where useful during validation;
- retire old objects only after instructor approval.

### 2.5 Scope Classification

The Raw-to-Bronze rebuild is treated as instructor-reference implementation work. It is not a student-facing answer key for Silver or Gold analytics and should remain focused on ingestion architecture, metadata, operational logging, and reproducibility.

---

## 3. Immediate Implications for Stage 1

Based on the Stage 0 decisions, Stage 1 should begin with:

1. a new release-specific configuration set under `config/raw_to_bronze/`;
2. a new source registry for the thirteen authoritative Parquet files;
3. configuration-driven naming for release-specific Raw and Bronze schemas;
4. a new Databricks pipeline workflow definition for `NAPA Raw to Bronze`;
5. reusable Python modules that do not depend on the legacy shared-schema notebook assumptions.

---

## 4. Files to Treat as Legacy Context

These files should be read for context only during the rebuild unless later explicitly migrated:

- `notebooks/01_setup_catalog.py`
- `notebooks/01_ingest_raw_to_bronze.py`
- `notebooks/_shared_pipeline_config.py`
- `src/napa_pipeline/config.py`

Their current behavior should not be assumed to satisfy the Raw-to-Bronze specification.

---

## 5. Stage 0 Completion Statement

Stage 0 is complete when:

- the restart boundary is documented;
- the legacy Bronze assumptions are identified;
- the target release-specific architecture is recorded;
- current Databricks objects are explicitly preserved pending validation;
- Stage 1 can begin without ambiguity about which implementation path is authoritative.
