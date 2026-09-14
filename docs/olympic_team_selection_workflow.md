# Olympic Team Selection Workflow

**Purpose:** This instructor-only Databricks script converts governed Gold team scorecards into reproducible Olympic doubles team sets without creating or recombining teams.

Deploy the Databricks Asset Bundle, then run the `NAPA Olympic Team Selection` job after the Silver-to-Gold workflow has published `team_selection_scorecards`.

```text
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev napa_olympic_team_selection --params release_name=napa_250k,team_set_count=4
```

The script uses the established `release_name` values (`napa_5k`, `napa_50k`, or `napa_250k`) and requires a positive `team_set_count`. It writes the managed Gold table `<catalog>.<gold_schema>.olympic_team_selections`, one row per country/division/team. A count of four yields 24 rows: four ranks in each USA and Canada men's, women's, and mixed group.

The only analytical input is the governed Gold `team_selection_scorecards` table. The selector filters existing eligible teams, orders by `final_team_selection_score`, then breaks ties by `combined_team_confidence` descending and canonical `team_id` ascending. It does not calculate a new score, alter membership, or impose player-overlap constraints.

Before writing, the script verifies the scorecard contains governed regional, age, fatigue, and composite context factors from `team_context_adjustment_features`. Run the main Silver-to-Gold workflow successfully for the same release before running this job.

It also rejects missing required groups, null team numbers, insufficient eligible teams, output row-count mismatches, rank gaps, duplicate team numbers within a group, and selections that cannot trace to an existing scorecard team ID.
