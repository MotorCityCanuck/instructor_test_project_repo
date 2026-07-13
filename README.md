
# NAPA Olympic Analytics Platform

## License and Use

This repository is provided as an instructional template for DSB6000 Data Science Strategy & Leadership. Students enrolled in the course may copy and modify the template for their assigned team project. Redistribution, commercial use, or publication outside the course requires permission from the instructor.

See `LICENSE.md` for details.

**Purpose:** This repository is a professional starting scaffold for student consulting teams building the DSB6000 NAPA Olympic Analytics Platform case study. It provides structure, documentation templates, and placeholder files only; teams must design, implement, validate, and explain their own pipeline logic, analytical methods, outputs, and recommendations.

## Team Placeholders

Team Name: _Replace with your consulting team name_  
Team Members: _Replace with names and roles_  
Repository Owner: _Replace with GitHub owner or organization_  
Primary Contact: _Replace with team contact name and email_

## Business Scenario Summary

Student teams act as analytics consulting firms responding to a fictional North American Pickleball Association request for a prototype Olympic Analytics Platform. The platform is expected to support athlete evaluation, doubles partnership analysis, national rankings, Olympic roster recommendations, tournament candidate selection, data quality confidence, governance documentation, and executive reporting.

## What This Template Includes

- A clean repository structure aligned to GitHub, Databricks, and medallion architecture concepts.
- Markdown documentation templates for architecture, methodology, governance, lineage, data quality, AI usage, and runbook evidence.
- Placeholder notebook, Python, SQL, configuration, and test files.
- Dataset specification reference documentation to orient student teams before implementation.
- Empty output and deliverable folders for milestone evidence.

## What This Template Does Not Include

- A completed implementation.
- Working ingestion, Bronze, Silver, or Gold pipeline logic.
- Working data quality checks or confidence calculations.
- Working scoring, ranking, partnership, or roster-selection methods.
- Completed dashboards, rankings, scorecards, or recommendations.
- Any raw or generated dataset files.

## Repository Structure Overview

```text
config/         Example project and dataset configuration
data/           Local data placement guidance only; no source data committed
notebooks/      Databricks-style notebook outlines for milestone workflow
src/            Reserved for student-developed reusable code
sql/            Comment-only SQL planning files
docs/           Project documentation templates and dataset reference
outputs/        Generated evidence folders for validation and analytics outputs
tests/          Reserved for student-developed tests
deliverables/   Milestone evidence folders
```

## Dataset Handling Note

No raw dataset files should be committed to GitHub. Teams should store local source files under the documented `data/raw/` convention or approved Databricks storage locations, then document the actual storage approach in the team runbook.

## Quick Start Steps

1. Copy this template into your team repository.
2. Update the team placeholders in this README and core documentation files.
3. Review [`docs/assignment_context.md`](docs/assignment_context.md) and [`docs/dataset_specification.md`](docs/dataset_specification.md).
4. Copy the example configuration files to local variants and adapt them for your environment.
5. Place source datasets locally or configure Databricks storage without committing raw files.
6. Design your Bronze, Silver, Gold, data quality, and analytical methodology before implementing code.
7. Use GitHub commits and milestone folders to preserve evidence for review.

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
- [`docs/data_quality_rules.md`](docs/data_quality_rules.md): planned quality controls.
- [`docs/data_quality_report.md`](docs/data_quality_report.md): milestone quality findings and confidence impact.
- [`docs/lineage.md`](docs/lineage.md): source-to-output traceability.
- [`docs/governance.md`](docs/governance.md): ownership, stewardship, classification, and access approach.
- [`docs/analytical_methodology.md`](docs/analytical_methodology.md): team-designed evaluation and recommendation methods.
- [`docs/ai_usage_summary.md`](docs/ai_usage_summary.md): AI-assisted development evidence.
- [`docs/runbook.md`](docs/runbook.md): setup and execution instructions.

This template is suitable to publish as a GitHub Template Repository after the team placeholders are reviewed and any organization-specific notes are added. Students remain responsible for all pipeline logic, quality checks, analytical methods, outputs, and recommendations.
