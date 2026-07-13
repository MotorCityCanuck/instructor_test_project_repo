# Data Folder Guide

**Purpose:** This document explains how student teams should handle NAPA dataset files for the template repository. The template provides folder conventions and documentation prompts only; teams must manage actual dataset placement, validation, and environment-specific storage decisions themselves.

## Data Is Not Stored in GitHub

Raw source datasets and large generated data artifacts must not be committed to this repository. The delivered dataset files remain the source of truth and should be stored locally or in approved Databricks locations outside Git version control.

## Local Folder Convention

Teams may place local source files under the following structure when working outside Databricks:

```text
data/raw/napa_5k/
data/raw/napa_50k/
data/raw/napa_250k/
```

## Databricks Storage Convention

Databricks storage locations may differ from the local folder convention. Teams should document the actual storage approach, mounted paths, volumes, workspace files, or other approved locations in `docs/runbook.md`.

## Active Dataset Configuration

Use `config/dataset_config.example.yml` as the starting point for dataset switching. Copy it to a local configuration file, adapt the paths for your environment, and avoid committing personal or workspace-specific settings.

## What May Be Committed

- Markdown documentation and milestone evidence summaries.
- Small screenshots or lightweight review artifacts when appropriate.
- Example configuration templates.
- Placeholder or student-developed code that does not embed raw source data.

## What Must Not Be Committed

- Raw dataset files.
- Large generated datasets or exports.
- Personal environment paths or secrets.
- Temporary extracts that recreate source datasets in GitHub.
