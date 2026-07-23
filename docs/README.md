# Documentation Set

**Purpose:** This document explains the documentation set included with the NAPA Olympic Analytics Platform template. The repository provides structure and prompts only; student teams must complete, maintain, and defend their own project documentation as the engagement evolves.

| Document | Purpose | When to update |
|---|---|---|
| assignment_context.md | Capture the business scenario, stakeholder needs, and team interpretation of the client ask. | At project start and whenever scope interpretation changes. |
| dataset_specification.md | Record dataset release strategy, source file expectations, and validation responsibilities. | Before ingestion and after any source validation findings. |
| architecture.md | Document the proposed technical architecture and tool roles. | After major design decisions or workflow changes. |
| medallion_design.md | Explain Raw, Bronze, Silver, and Gold layer intent and scope. | When medallion design or milestone evidence changes. |
| data_dictionary.md | Define important tables, fields, and business meanings. | After schema discovery and whenever entities change. |
| data_quality_rules.md | Define planned data quality checks and severity expectations. | Before rule implementation and when rules change. |
| data_quality_report.md | Summarize run-specific quality findings and confidence impact. | After each milestone run or major validation cycle. |
| lineage.md | Trace data flow from source files to final deliverables. | When new transformations or outputs are introduced. |
| governance.md | Record governance, ownership, classification, and access assumptions. | When governance decisions or risks change. |
| analytical_methodology.md | Explain team-designed evaluation, ranking, and recommendation methods. | Before presenting analytical outputs and after refinements. |
| ai_usage_summary.md | Document AI-assisted work and human review evidence. | Whenever AI-assisted development materially contributes to work. |
| runbook.md | Provide reproducible setup and execution instructions. | As setup steps or environment assumptions change. |
| raw_to_bronze_workflow.md | Deploy, run, inspect, and rerun the Raw-to-Bronze Databricks Workflow. | When workflow task wiring, parameters, deployment, or audit behavior changes. |
| NAPA_Bronze_to_Silver_Implementation_Decisions.md | Freeze the instructor Bronze-to-Silver repo shape, package layout, config layout, and serverless workflow assumptions before implementation expands. | Before major Bronze-to-Silver implementation work and whenever foundational architecture decisions change. |
| gold_contract_workflow.md | Define the contract-first workflow for Gold development so deployed Databricks schemas stay ahead of implementation assumptions. | Before each new Gold phase and after any Bronze-to-Silver contract change. |
| gold_target_schema_registry.md | Track the evolving Gold target table contracts, phase status, dependencies, and unresolved derivations. | Before and after each Gold table implementation or contract change. |
