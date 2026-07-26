
# NAPA Olympic Analytics Platform

## License and Use

This repository is provided as an instructional template for DSB6000 Data Science Strategy & Leadership. Students enrolled in the course may copy and modify the template for their assigned team project. Redistribution, commercial use, or publication outside the course requires permission from the instructor.

See `LICENSE.md` for details.

**Purpose:** This repository began as a professional starting scaffold for student consulting teams building the DSB6000 NAPA Olympic Analytics Platform case study. This instructor test branch now also contains executable instructor reference pipelines for Raw-to-Bronze, Bronze-to-Silver, Silver-to-Gold, and standalone Gold audit validation. Student-facing copies should continue to preserve meaningful student design and implementation work.

## Instructor Implementation Status

The current instructor implementation includes:

- Databricks Asset Bundle workflow resources for Raw-to-Bronze, Bronze-to-Silver, Silver-to-Gold, and Gold audit validation.
- Configuration-driven release support for `napa_5k`, `napa_50k`, and `napa_250k`.
- Reusable Python modules under `src/napa_pipeline/`.
- Operations/audit logging in `workspace.instructor_ops`.
- Implemented table catalogs documented in [`docs/implemented_layer_catalog.md`](docs/implemented_layer_catalog.md).

Primary workflow docs:

- [`docs/raw_to_bronze_workflow.md`](docs/raw_to_bronze_workflow.md)
- [`docs/bronze_to_silver_workflow.md`](docs/bronze_to_silver_workflow.md)
- [`docs/silver_to_gold_workflow.md`](docs/silver_to_gold_workflow.md)
- [`docs/silver_to_gold_audit_workflow.md`](docs/silver_to_gold_audit_workflow.md)

## Team Placeholders

Team Name: _Replace with your consulting team name_  
Team Members: _Replace with names and roles_  
Repository Owner: _Replace with GitHub owner or organization_  
Primary Contact: _Replace with team contact name and email_

## Business Scenario Summary

Student teams act as analytics consulting firms responding to a fictional North American Pickleball Association request for a prototype Olympic Analytics Platform. The platform is expected to support athlete evaluation, doubles partnership analysis, national rankings, Olympic roster recommendations, tournament candidate selection, data quality confidence, governance documentation, and executive reporting.

## What The Student Template Includes

- A clean repository structure aligned to GitHub, Databricks, and medallion architecture concepts.
- Markdown documentation templates for architecture, methodology, governance, lineage, data quality, AI usage, and runbook evidence.
- Placeholder notebook, Python, SQL, configuration, and test files.
- Dataset specification reference documentation to orient student teams before implementation.
- Empty output and deliverable folders for milestone evidence.

## Student-Facing Boundary

Student-facing distributions should not include final analytical conclusions, answer keys, hidden simulation controls, or instructor-only validation shortcuts unless explicitly approved by the instructor. No raw or generated dataset files should be committed.

## Repository Structure Overview

```text
config/         Pipeline configuration and Databricks workflow resources
data/           Local data placement guidance only; no source data committed
notebooks/      Databricks Python script task entrypoints and legacy outlines
src/            Reusable instructor reference pipeline modules
sql/            Comment-only SQL planning files
docs/           Project documentation, implementation catalogs, and runbooks
outputs/        Generated evidence folders for validation and analytics outputs
tests/          Automated unit and workflow-definition tests
deliverables/   Milestone evidence folders
```

## Dataset Handling Note

No raw dataset files should be committed to GitHub. Teams should store local source files under the documented `data/raw/` convention or approved Databricks storage locations, then document the actual storage approach in the team runbook.

## Quick Start Steps

1. Confirm whether you are using the instructor reference branch or a student-facing template export.
2. Update the team placeholders in this README and core documentation files.
3. Review [`docs/assignment_context.md`](docs/assignment_context.md) and [`docs/dataset_specification.md`](docs/dataset_specification.md).
4. For instructor validation, review [`docs/runbook.md`](docs/runbook.md) and [`docs/implemented_layer_catalog.md`](docs/implemented_layer_catalog.md).
5. Place source datasets locally or configure Databricks storage without committing raw files.
6. Use GitHub commits and milestone folders to preserve evidence for review.

## Configuration Overview

- `config/dataset_config.example.yml` documents dataset switching and target catalog/schema placeholders.
- `config/project_config.example.yml` documents team, country scope, category scope, and milestone dataset alignment.
- Teams are responsible for any additional configuration they introduce and must be able to explain it.

## Expected Milestone Workflow

1. Milestone 1: establish dataset readiness, ingestion evidence, Bronze planning, and foundational documentation.
2. Milestone 2: build and validate Silver entities, data quality processes, and engineering evidence on the 50K dataset.
3. Milestone 3: produce Gold analytical products, final recommendations, and executive-ready deliverables on the 250K dataset.

## Responsible AI-Assisted Development Note

AI-assisted tools may help teams draft code, documentation, tests, and design ideas, but students remain fully accountable for correctness, originality, explainability, validation, and professional judgment. All important AI-assisted outputs should be reviewed, tested, and documented in [`docs/ai_usage_summary.md`](docs/ai_usage_summary.md).

## Suggested Beginner-Friendly Git Workflow

1. Create a feature branch for each major workstream.
2. Keep commits small and descriptive.
3. Use pull requests or structured peer review even if your team is small.
4. Preserve milestone evidence in both commit history and `deliverables/` folders.
5. Avoid committing secrets, local configuration files, and data extracts.

## Documentation Map

- [`docs/assignment_context.md`](docs/assignment_context.md): business scenario and consulting interpretation.
- [`docs/dataset_specification.md`](docs/dataset_specification.md): dataset release strategy and source file reference.
- [`docs/architecture.md`](docs/architecture.md): technical design decisions.
- [`docs/medallion_design.md`](docs/medallion_design.md): Raw, Bronze, Silver, Gold approach.
- [`docs/data_dictionary.md`](docs/data_dictionary.md): field and table definitions.
- [`docs/implemented_layer_catalog.md`](docs/implemented_layer_catalog.md): implemented Raw, Bronze, Silver, Gold, and operations catalogs.
- [`docs/data_quality_rules.md`](docs/data_quality_rules.md): planned quality controls.
- [`docs/data_quality_report.md`](docs/data_quality_report.md): milestone quality findings and confidence impact.
- [`docs/lineage.md`](docs/lineage.md): source-to-output traceability.
- [`docs/governance.md`](docs/governance.md): ownership, stewardship, classification, and access approach.
- [`docs/analytical_methodology.md`](docs/analytical_methodology.md): team-designed evaluation and recommendation methods.
- [`docs/ai_usage_summary.md`](docs/ai_usage_summary.md): AI-assisted development evidence.
- [`docs/runbook.md`](docs/runbook.md): setup and execution instructions.

This template is suitable to publish as a GitHub Template Repository after the team placeholders are reviewed and any organization-specific notes are added. Students remain responsible for all pipeline logic, quality checks, analytical methods, outputs, and recommendations.

Before publishing a student-facing copy, review instructor-only implementation files and documentation to decide what should be retained, removed, or converted back to scaffolding.
