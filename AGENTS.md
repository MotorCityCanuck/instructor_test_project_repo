# AGENTS.md

## Purpose

This repository supports the instructor test build for the North American Pickleball Association (NAPA) Olympic analytics case study. It is intended to validate the technical environment, repository structure, Databricks workflow, data engineering approach, and student-facing instructions before the project is assigned to students.

AI coding agents working in this repository must support the project without silently redefining the assignment, overengineering the solution, or completing analytical decisions that should remain the responsibility of students. The agent should behave as a careful technical collaborator: inspect first, explain assumptions, make focused changes, preserve existing structure, and validate its work.

---

## 1. Core Operating Principles

### 1.1 Inspect before changing

Before editing files, the agent should:

1. Read this `AGENTS.md`.
2. Inspect the repository structure.
3. Read the root `README.md`.
4. Review relevant documentation under `docs/`.
5. Review existing notebooks, source files, tests, and configuration.
6. Identify the current Git branch and working-tree status.
7. Confirm whether the requested change affects instructor-only materials, student-facing materials, or both.

Do not assume the repository structure, naming conventions, data locations, or Databricks configuration.

### 1.2 Make the smallest appropriate change

Prefer targeted changes over broad rewrites.

When asked to modify an existing document, notebook, script, or configuration file:

- preserve unaffected content;
- preserve established formatting and naming;
- avoid reorganizing unrelated files;
- avoid introducing new frameworks or dependencies without a clear need;
- do not replace working code merely to impose a different style;
- do not make speculative improvements outside the request.

### 1.3 Do not do the project for the students

This repository may include examples, templates, and directional scaffolding. The agent must not convert that scaffolding into a completed student solution unless the user explicitly asks for an instructor reference implementation.

For student-facing artifacts, the agent should avoid providing:

- completed business conclusions;
- final roster selections;
- final model choices presented as mandatory;
- production-ready feature engineering implementations;
- finished end-to-end notebooks that remove meaningful student work;
- complete answers to milestone deliverables;
- hidden simulation parameters;
- grading shortcuts;
- fabricated results.

Appropriate student-facing support includes:

- folder and notebook structure;
- setup instructions;
- interface contracts;
- placeholder functions;
- pseudocode;
- examples using toy or synthetic sample data;
- validation checklists;
- directional comments;
- troubleshooting guidance;
- minimum viable patterns.

### 1.4 Preserve instructor control

The instructor remains the final authority for:

- assignment scope;
- milestone requirements;
- grading criteria;
- release sequencing;
- hidden factors;
- tournament simulation logic;
- expected deliverables;
- student guidance level.

If repository code or documentation conflicts with assignment materials, identify the conflict and stop before silently choosing one interpretation.

---

## 2. Repository Context

The NAPA case study is a graduate-level data science and technical leadership project involving a synthetic pickleball analytics platform.

The broader solution may include:

- staged monthly data releases;
- Parquet source files;
- raw, bronze, silver, and gold data layers;
- Delta Lake tables;
- data quality checks;
- player and team analytics;
- match prediction;
- doubles chemistry analysis;
- confidence scoring;
- explainable roster recommendations;
- longitudinal analysis;
- future-athlete identification;
- dashboards or summary outputs;
- instructor-run Monte Carlo tournament simulation.

The student project should demonstrate technical leadership, data engineering, analytics, reproducibility, explainability, and governance. The repository should support those goals without prescribing every implementation decision.

---

## 3. Expected Repository Organization

The agent must inspect the actual repository and adapt to it. A typical structure may resemble:

```text
.
├── AGENTS.md
├── README.md
├── LICENSE
├── .gitignore
├── pyproject.toml
├── requirements.txt
├── configs/
│   ├── dev/
│   └── prod/
├── docs/
│   ├── architecture/
│   ├── data/
│   ├── development/
│   ├── governance/
│   └── student_guidance/
├── notebooks/
│   ├── 00_setup/
│   ├── 01_raw/
│   ├── 02_bronze/
│   ├── 03_silver/
│   ├── 04_gold/
│   ├── 05_analysis/
│   └── 06_validation/
├── src/
│   └── napa/
│       ├── ingestion/
│       ├── quality/
│       ├── transformation/
│       ├── features/
│       ├── modeling/
│       ├── evaluation/
│       └── utilities/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── sample_data/
└── scripts/
```

Do not create this structure automatically if the repository already uses another approved structure.

---

## 4. Git and Branching Rules

### 4.1 Never work blindly on the current branch

Before making changes, check:

```bash
git status
git branch --show-current
git remote -v
```

If the working tree contains unrelated changes, report them before editing.

### 4.2 Branch conventions

Unless the repository specifies otherwise, use focused branch names such as:

```text
feature/bronze-ingestion
feature/schema-setup
feature/data-quality-rules
fix/databricks-path-resolution
docs/student-setup-guide
chore/dependency-cleanup
```

Do not commit directly to `main` unless explicitly instructed.

### 4.3 Commit discipline

Commits should be:

- focused;
- understandable;
- limited to one logical change;
- free of generated clutter;
- free of data files that should not be stored in Git.

Recommended commit style:

```text
Add Databricks schema initialization notebook
Fix release sequence validation
Clarify student Git workflow
Add unit tests for path configuration
```

Avoid vague messages such as:

```text
updates
changes
fix stuff
final
```

### 4.4 Do not commit large or sensitive data

Do not commit:

- full NAPA datasets;
- generated Delta table storage;
- Databricks checkpoints;
- credentials;
- tokens;
- `.env` files containing secrets;
- personal access tokens;
- exported workspace metadata containing private identifiers;
- large logs;
- model binaries unless explicitly approved.

Use `.gitignore` and placeholder files where appropriate.

---

## 5. Databricks Conventions

### 5.1 Catalog, schema, and table naming

Use three-part names where Unity Catalog is available:

```text
catalog.schema.table
```

A typical instructor test configuration may use:

```text
workspace.napa_raw
workspace.napa_bronze
workspace.napa_silver
workspace.napa_gold
```

Do not assume `workspace` is the catalog. Inspect the environment or configuration first.

Preferred pattern:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.napa_raw;
CREATE SCHEMA IF NOT EXISTS workspace.napa_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.napa_silver;
CREATE SCHEMA IF NOT EXISTS workspace.napa_gold;
```

Use configuration variables rather than hard-coding catalog names throughout the codebase.

### 5.2 Notebook responsibilities

Notebooks should orchestrate work, not contain all reusable logic.

Preferred division:

- notebooks: sequence, parameters, explanations, execution flow;
- `src/`: reusable transformations, validators, utilities, and business logic;
- `tests/`: automated verification;
- `configs/`: environment-specific values;
- `docs/`: design rationale and operating guidance.

### 5.3 Notebook naming

Use ordered, descriptive names. For example:

```text
00_setup/01_create_schemas
01_raw/01_register_release
02_bronze/01_ingest_players
03_silver/01_standardize_players
04_gold/01_build_player_summary
06_validation/01_pipeline_checks
```

Do not encode dates in notebook names unless the notebook is release-specific by design.

### 5.4 Notebook parameters

Use widgets or configuration values for items such as:

- catalog;
- environment;
- release sequence;
- source path;
- target schema;
- run mode;
- validation mode.

Example:

```python
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("release_sequence", "")
dbutils.widgets.text("source_path", "")
```

Validate required parameters before execution.

### 5.5 Avoid environment-specific hard-coding

Do not hard-code:

- user workspace paths;
- personal email addresses;
- local Windows paths;
- DBFS paths tied to one user;
- secrets;
- cluster IDs;
- warehouse IDs.

Prefer configuration and documented defaults.

---

## 6. Data Layer Expectations

### 6.1 Raw layer

The raw layer should preserve the source release as received.

Typical responsibilities:

- register the release;
- capture source metadata;
- preserve file-level lineage;
- record ingestion timestamps;
- avoid business transformations;
- retain source columns and values.

The agent should not rename or normalize source fields in raw unless the repository specification explicitly requires it.

### 6.2 Bronze layer

The bronze layer should create reliable Delta representations of source data.

Typical responsibilities:

- read Parquet files;
- write Delta tables;
- add technical metadata;
- preserve source fidelity;
- apply basic schema validation;
- detect malformed or missing inputs;
- support idempotent reprocessing.

Bronze should not become a heavily curated analytical layer.

### 6.3 Silver layer

The silver layer should contain validated, standardized, and integrated data.

Typical responsibilities:

- type enforcement;
- deduplication;
- standardization;
- referential integrity checks;
- conformance across releases;
- business-rule validation;
- integration of related entities;
- handling active/inactive records;
- preparation for feature engineering.

### 6.4 Gold layer

The gold layer should contain business-ready outputs.

Typical examples:

- player performance summaries;
- team chemistry measures;
- model-ready feature tables;
- roster decision support tables;
- longitudinal trend tables;
- dashboard-ready aggregates;
- confidence and explainability outputs.

Do not assume the exact gold tables without reading the project requirements.

### 6.5 Idempotency

Pipeline steps should be safe to rerun.

Prefer deterministic behavior using:

- `MERGE`;
- partition replacement;
- release-based overwrite;
- stable keys;
- explicit deduplication;
- audit tables;
- run identifiers.

Avoid uncontrolled append-only behavior unless the data model specifically requires it.

---

## 7. Data Release Handling

The NAPA project may use an initial release followed by monthly incremental releases.

The agent should preserve:

- release sequence;
- source release date;
- file name;
- ingestion timestamp;
- record provenance;
- active/inactive status changes;
- new and changed records.

Recommended technical metadata fields may include:

```text
_source_file
_release_sequence
_release_date
_ingested_at
_pipeline_run_id
_record_hash
```

Only add metadata fields that are compatible with the repository's documented schema strategy.

Do not infer that every monthly file is a complete snapshot or an incremental delta. Confirm from documentation before implementing merge logic.

---

## 8. Schema Management

### 8.1 Schema definitions

Where practical, define schemas explicitly rather than relying entirely on inference.

Use one of the following, based on repository conventions:

- PySpark `StructType`;
- SQL DDL;
- documented YAML or JSON schema;
- table contracts.

### 8.2 Schema evolution

Do not enable permissive schema evolution by default.

Before accepting a schema change:

1. compare source and expected schemas;
2. classify the difference;
3. determine whether it is planned;
4. update the documented contract;
5. add or update tests;
6. record the decision.

Unexpected schema drift should fail clearly or be quarantined.

### 8.3 Data contracts

Where the repository contains dataset specifications, treat them as authoritative.

Do not silently alter:

- field names;
- types;
- nullability;
- keys;
- enumerations;
- release semantics;
- entity relationships.

---

## 9. Data Quality Rules

Data quality logic should be explicit, testable, and traceable.

Potential rule categories include:

- required fields;
- valid identifiers;
- uniqueness;
- accepted ranges;
- valid enumerations;
- date consistency;
- referential integrity;
- duplicate detection;
- release sequencing;
- impossible match outcomes;
- team composition validity;
- country eligibility;
- active/inactive consistency.

A good validation result should identify:

```text
rule_id
rule_name
dataset
release_sequence
severity
failed_record_count
run_id
checked_at
```

Do not fabricate thresholds. Use documented thresholds or clearly label proposed defaults.

---

## 10. Testing Standards

### 10.1 Test meaningful behavior

Tests should cover:

- schema contracts;
- transformation logic;
- duplicate handling;
- null handling;
- merge behavior;
- release ordering;
- idempotency;
- referential integrity;
- configuration parsing;
- representative edge cases.

### 10.2 Keep tests small and deterministic

Use small synthetic fixtures. Do not require the full production-sized dataset for unit tests.

Tests should not depend on:

- current time without control;
- network access;
- personal paths;
- hidden state;
- unpredictable record ordering.

### 10.3 Suggested test commands

Inspect the project configuration before running commands. Common commands may include:

```bash
pytest
pytest tests/unit
pytest -q
python -m pytest
```

If formatting or linting tools exist, use the repository's configured commands, such as:

```bash
ruff check .
ruff format --check .
black --check .
mypy src
```

Do not introduce a new linting stack solely because it is preferred.

### 10.4 Validation before completion

Before declaring work complete:

1. run relevant tests;
2. run formatting or lint checks if configured;
3. inspect changed files;
4. review the diff;
5. confirm no secrets or data were added;
6. verify documentation affected by the change;
7. report any checks that could not be run.

---

## 11. Python Standards

### 11.1 General style

Use:

- clear function names;
- type hints where practical;
- docstrings for public functions;
- focused modules;
- explicit error handling;
- meaningful exceptions;
- minimal global state.

Prefer readable code over clever code.

### 11.2 Spark practices

Prefer built-in Spark functions over Python UDFs.

Use:

```python
from pyspark.sql import functions as F
```

Avoid:

- `collect()` on large datasets;
- unnecessary `toPandas()`;
- repeated full-table scans;
- uncontrolled caching;
- Python loops over Spark rows;
- hard-coded repartition counts without evidence.

### 11.3 Logging

Use structured, concise logging.

Log:

- run start and end;
- parameters;
- input paths;
- release sequence;
- table targets;
- row counts where appropriate;
- validation outcomes;
- failures.

Do not log secrets or full sensitive records.

---

## 12. SQL Standards

Use explicit column lists.

Avoid:

```sql
SELECT *
```

in durable production transformations unless there is a documented reason.

Use consistent naming and formatting.

Prefer:

```sql
SELECT
    player_id,
    country_code,
    rating,
    rating_confidence
FROM workspace.napa_silver.players
WHERE is_active = true;
```

Use fully qualified table names in pipeline code unless a notebook deliberately establishes and documents the current catalog and schema.

---

## 13. Configuration and Secrets

Configuration values should be externalized where reasonable.

Potential configuration items include:

- catalog;
- schema names;
- source paths;
- checkpoint paths;
- release sequence;
- environment;
- feature flags;
- validation thresholds.

Secrets must use an approved secret mechanism. Never place secrets directly in:

- notebooks;
- source files;
- Markdown;
- Git configuration;
- sample environment files with real values.

A sample file may use placeholders:

```text
DATABRICKS_HOST=<your-workspace-url>
DATABRICKS_TOKEN=<set-locally-do-not-commit>
```

---

## 14. Documentation Standards

Documentation should be professional, concise, and fact-based.

Every substantive technical document should begin with a short purpose statement.

Where useful, include:

- audience;
- prerequisites;
- assumptions;
- step-by-step procedure;
- expected result;
- validation step;
- troubleshooting;
- limitations.

For student-facing documentation:

- explain direction without providing final answers;
- separate required steps from optional approaches;
- identify where teams must make decisions;
- avoid presenting one implementation as the only valid solution unless mandated.

Do not let documentation drift from the actual repository.

---

## 15. Instructor-Only vs Student-Facing Content

The agent should distinguish between:

### Instructor-only content

May include:

- full validation implementation;
- hidden test cases;
- reference solutions;
- simulation controls;
- answer keys;
- grading aids;
- private configuration;
- release generation logic.

### Student-facing content

May include:

- setup guides;
- templates;
- checklists;
- architecture prompts;
- interface contracts;
- incomplete examples;
- troubleshooting;
- milestone guidance.

Do not move instructor-only material into public or student-facing folders without explicit approval.

---

## 16. AI-Assisted Development Expectations

When an AI agent produces code or documentation, it should make its assumptions visible.

For nontrivial work, report:

- files inspected;
- files changed;
- design decisions;
- assumptions;
- tests run;
- tests not run;
- remaining risks.

The agent should not claim a notebook, pipeline, or test works unless it was actually executed or otherwise validated.

Avoid hallucinating:

- file names;
- table names;
- catalog names;
- dataset fields;
- test results;
- Databricks capabilities;
- assignment requirements.

---

## 17. Error Handling and Failure Behavior

Fail clearly when:

- a required source file is missing;
- a required parameter is empty;
- release order is invalid;
- schema drift is unexpected;
- a target catalog or schema is inaccessible;
- keys are duplicated unexpectedly;
- referential integrity fails above an approved threshold;
- credentials are unavailable.

Error messages should explain:

1. what failed;
2. where it failed;
3. the likely cause;
4. the next corrective action.

Do not suppress exceptions merely to keep the pipeline running.

---

## 18. Performance and Scale

The NAPA dataset may become large. Code should be designed with scale in mind, but optimization should be evidence-based.

Consider:

- partition pruning;
- appropriate Delta layout;
- incremental processing;
- selective caching;
- avoiding unnecessary shuffles;
- filtering early;
- compact file management;
- efficient merge keys;
- avoiding repeated recomputation.

Do not prematurely introduce advanced optimization that makes the student solution difficult to understand.

---

## 19. Reproducibility

A clean clone of the repository should contain enough information to:

- understand the project;
- configure the environment;
- locate external data;
- create required schemas;
- run the pipeline in order;
- execute tests;
- reproduce outputs from the same release and configuration.

Where external data cannot be stored in Git, document:

- expected location;
- expected file names;
- expected structure;
- how to validate arrival;
- how to configure the path.

---

## 20. File Modification Rules

Before modifying a file:

1. read the full file;
2. identify its purpose;
3. check references from other files;
4. preserve its public interface where possible;
5. update tests and documentation when behavior changes.

Do not edit generated files directly unless the repository explicitly requires it.

Do not rename files casually. File names may be referenced by:

- Databricks workflows;
- documentation;
- imports;
- tests;
- milestone instructions;
- GitHub links.

---

## 21. Pull Request Expectations

A pull request should explain:

- the problem;
- the change;
- why the approach was chosen;
- files affected;
- how it was tested;
- limitations;
- any follow-up work.

Suggested structure:

```markdown
## Summary

## Changes

## Validation

## Risks or Limitations

## Documentation Updated
```

Do not combine unrelated changes into one pull request.

---

## 22. Agent Completion Checklist

Before finishing any coding task, verify:

- [ ] The request was interpreted correctly.
- [ ] The repository instructions were followed.
- [ ] The current branch and Git status were checked.
- [ ] Only relevant files were changed.
- [ ] Existing structure and style were preserved.
- [ ] No large data or secrets were added.
- [ ] Student-facing material does not complete the assignment for students.
- [ ] Relevant tests were run.
- [ ] Formatting or lint checks were run when configured.
- [ ] The final diff was reviewed.
- [ ] Documentation was updated where needed.
- [ ] Assumptions and limitations were reported.
- [ ] No success claims were made without validation.

---

## 23. Preferred Agent Response Format

At the end of a task, summarize work using:

```text
Summary
- What changed

Files changed
- path/to/file

Validation
- Tests or checks run

Assumptions
- Any assumptions made

Remaining items
- Anything unresolved
```

Keep the summary factual. Do not overstate completeness.

---

## 24. First Actions for Any New Task

When beginning work in this repository, use the following sequence:

```bash
git status
git branch --show-current
git remote -v
```

Then inspect:

```text
AGENTS.md
README.md
docs/
pyproject.toml or requirements.txt
relevant notebooks/
relevant src/
relevant tests/
```

Only after inspection should the agent propose or make changes.

---

## 25. Final Authority

When this file conflicts with a direct instruction from the repository owner or instructor, the direct instruction takes precedence.

When the direct instruction appears to conflict with assignment requirements, repository integrity, security, or student-facing boundaries, pause and identify the conflict before proceeding.
