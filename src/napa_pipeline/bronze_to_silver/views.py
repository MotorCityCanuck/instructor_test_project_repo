"""Convenience-view builders for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from typing import Any

from napa_pipeline.bronze_to_silver.environment import ReleaseEnvironment


def build_vw_players_current(
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    """Return one row per currently active player."""
    current_rows = [row for row in players_rows if row.get("active_flag") is True]
    return tuple(
        {
            "player_id": row["player_id"],
            "player_sk": row["player_sk"],
            "display_name": row.get("display_name"),
            "home_region_id": row.get("home_region_id"),
            "country_code": row.get("country_code"),
            "age": row.get("age"),
            "active_flag": row.get("active_flag"),
        }
        for row in sorted(current_rows, key=lambda item: str(item["player_id"]))
    )


def build_vw_players_current_sql(environment: ReleaseEnvironment) -> str:
    """Return SQL for the current active-player convenience view."""
    players_fqn = _silver_table_fqn(environment, "players")
    return f"""
SELECT
    player_id,
    player_sk,
    display_name,
    home_region_id,
    country_code,
    age,
    active_flag
FROM {players_fqn}
WHERE active_flag = true
""".strip()


def build_vw_current_team_memberships(
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    """Return current team-membership rows only."""
    current_rows = [
        row
        for row in team_memberships_rows
        if row.get("current_membership_flag") is True
    ]
    return tuple(
        sorted(
            current_rows,
            key=lambda item: (str(item["team_id"]), str(item["player_id"])),
        )
    )


def build_vw_current_team_memberships_sql(environment: ReleaseEnvironment) -> str:
    """Return SQL for the current team-membership convenience view."""
    memberships_fqn = _silver_table_fqn(environment, "team_memberships")
    return f"""
SELECT
    team_membership_id,
    team_membership_sk,
    player_id,
    player_sk,
    team_id,
    team_sk,
    membership_start_date,
    membership_end_date,
    membership_duration_days,
    current_membership_flag,
    membership_overlap_flag,
    player_role,
    player_position,
    _pipeline_run_id,
    _pipeline_version,
    _source_dataset,
    _source_table,
    _load_ts,
    _record_hash
FROM {memberships_fqn}
WHERE current_membership_flag = true
""".strip()


def build_vw_team_rosters(
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    team_memberships_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    expected_roster_count: int,
) -> tuple[dict[str, Any], ...]:
    """Return one row per team with current roster details and cardinality status."""
    players_index = {str(row["player_id"]): row for row in players_rows}
    memberships_by_team: dict[str, list[dict[str, Any]]] = {}
    for membership in team_memberships_rows:
        if membership.get("current_membership_flag") is True:
            memberships_by_team.setdefault(str(membership["team_id"]), []).append(membership)

    result: list[dict[str, Any]] = []
    for team_row in sorted(teams_rows, key=lambda item: str(item["team_id"])):
        team_id = str(team_row["team_id"])
        memberships = sorted(
            memberships_by_team.get(team_id, []),
            key=lambda item: (str(item.get("player_position") or ""), str(item["player_id"])),
        )
        roster_players = []
        for membership in memberships:
            player_row = players_index.get(str(membership["player_id"]))
            roster_players.append(
                {
                    "player_id": membership["player_id"],
                    "player_sk": membership.get("player_sk"),
                    "display_name": player_row.get("display_name") if player_row else None,
                    "player_role": membership.get("player_role"),
                    "player_position": membership.get("player_position"),
                }
            )
        roster_count = len(roster_players)
        result.append(
            {
                "team_id": team_id,
                "team_sk": team_row["team_sk"],
                "team_name": team_row.get("team_name"),
                "team_category": team_row.get("team_category"),
                "team_status": team_row.get("team_status"),
                "current_roster_count": roster_count,
                "expected_roster_count": expected_roster_count,
                "roster_cardinality_status": (
                    "OK" if roster_count == expected_roster_count else "WARNING"
                ),
                "roster_players": tuple(roster_players),
            }
        )
    return tuple(result)


def build_vw_team_rosters_sql(
    environment: ReleaseEnvironment,
    *,
    expected_roster_count: int,
) -> str:
    """Return SQL for the team-roster convenience view."""
    teams_fqn = _silver_table_fqn(environment, "teams")
    players_fqn = _silver_table_fqn(environment, "players")
    memberships_fqn = _silver_table_fqn(environment, "team_memberships")
    return f"""
WITH current_memberships AS (
    SELECT
        tm.team_id,
        tm.team_sk,
        tm.player_id,
        tm.player_sk,
        tm.player_role,
        tm.player_position,
        p.display_name
    FROM {memberships_fqn} tm
    LEFT JOIN {players_fqn} p
        ON tm.player_id = p.player_id
    WHERE tm.current_membership_flag = true
),
roster_counts AS (
    SELECT
        team_id,
        COUNT(*) AS current_roster_count
    FROM current_memberships
    GROUP BY team_id
),
roster_players AS (
    SELECT
        team_id,
        ARRAY_SORT(
            COLLECT_LIST(
                NAMED_STRUCT(
                    'player_id', player_id,
                    'player_sk', player_sk,
                    'display_name', display_name,
                    'player_role', player_role,
                    'player_position', player_position
                )
            )
        ) AS roster_players
    FROM current_memberships
    GROUP BY team_id
)
SELECT
    t.team_id,
    t.team_sk,
    t.team_name,
    t.team_category,
    t.team_status,
    COALESCE(rc.current_roster_count, 0) AS current_roster_count,
    {expected_roster_count} AS expected_roster_count,
    CASE
        WHEN COALESCE(rc.current_roster_count, 0) = {expected_roster_count} THEN 'OK'
        ELSE 'WARNING'
    END AS roster_cardinality_status,
    COALESCE(rp.roster_players, ARRAY()) AS roster_players
FROM {teams_fqn} t
LEFT JOIN roster_counts rc
    ON t.team_id = rc.team_id
LEFT JOIN roster_players rp
    ON t.team_id = rp.team_id
""".strip()


def build_vw_match_results(
    matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_games_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    regions_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    """Return one row per match with operationally useful joined details."""
    teams_index = {str(row["team_id"]): row for row in teams_rows if row.get("team_id") not in (None, "")}
    regions_index = {str(row["region_id"]): row for row in regions_rows if row.get("region_id") not in (None, "")}
    batches_index = {str(row["batch_id"]): row for row in monthly_batches_rows if row.get("batch_id") not in (None, "")}

    match_teams_by_match: dict[str, list[dict[str, Any]]] = {}
    for row in match_teams_rows:
        match_teams_by_match.setdefault(str(row["match_id"]), []).append(row)

    match_games_by_match: dict[str, list[dict[str, Any]]] = {}
    for row in match_games_rows:
        match_games_by_match.setdefault(str(row["match_id"]), []).append(row)

    results: list[dict[str, Any]] = []
    for match_row in sorted(matches_rows, key=lambda item: str(item["match_id"])):
        match_id = str(match_row["match_id"])
        sides = sorted(
            match_teams_by_match.get(match_id, []),
            key=lambda item: int(item.get("team_number") or 0),
        )
        games = match_games_by_match.get(match_id, [])
        aggregate_scores = _aggregate_game_scores(games)
        region_row = regions_index.get(str(match_row["region_id"])) if match_row.get("region_id") else None
        batch_row = batches_index.get(str(match_row["batch_id"])) if match_row.get("batch_id") else None
        side_one = sides[0] if len(sides) > 0 else None
        side_two = sides[1] if len(sides) > 1 else None
        team_one = teams_index.get(str(side_one["team_id"])) if side_one and side_one.get("team_id") else None
        team_two = teams_index.get(str(side_two["team_id"])) if side_two and side_two.get("team_id") else None
        results.append(
            {
                "match_id": match_id,
                "match_sk": match_row["match_sk"],
                "match_date": match_row.get("match_date"),
                "competition_category": match_row.get("competition_category"),
                "match_status": match_row.get("match_status"),
                "winning_team_number": match_row.get("winning_team_number"),
                "team_one_id": side_one.get("team_id") if side_one else None,
                "team_one_name": team_one.get("team_name") if team_one else None,
                "team_two_id": side_two.get("team_id") if side_two else None,
                "team_two_name": team_two.get("team_name") if team_two else None,
                "team_one_total_score": aggregate_scores["team_one_total_score"],
                "team_two_total_score": aggregate_scores["team_two_total_score"],
                "game_count": len(games),
                "region_id": match_row.get("region_id"),
                "region_name": region_row.get("region_name") if region_row else None,
                "batch_id": match_row.get("batch_id"),
                "batch_date": batch_row.get("batch_date") if batch_row else None,
            }
        )
    return tuple(results)


def build_vw_match_results_sql(environment: ReleaseEnvironment) -> str:
    """Return SQL for the match-results convenience view."""
    matches_fqn = _silver_table_fqn(environment, "matches")
    match_teams_fqn = _silver_table_fqn(environment, "match_teams")
    match_games_fqn = _silver_table_fqn(environment, "match_games")
    teams_fqn = _silver_table_fqn(environment, "teams")
    regions_fqn = _silver_table_fqn(environment, "regions")
    batches_fqn = _silver_table_fqn(environment, "monthly_batches")
    return f"""
WITH side_one AS (
    SELECT
        mt.match_id,
        mt.team_id AS team_one_id,
        t.team_name AS team_one_name
    FROM {match_teams_fqn} mt
    LEFT JOIN {teams_fqn} t
        ON mt.team_id = t.team_id
    WHERE mt.team_number = 1
),
side_two AS (
    SELECT
        mt.match_id,
        mt.team_id AS team_two_id,
        t.team_name AS team_two_name
    FROM {match_teams_fqn} mt
    LEFT JOIN {teams_fqn} t
        ON mt.team_id = t.team_id
    WHERE mt.team_number = 2
),
game_scores AS (
    SELECT
        match_id,
        SUM(COALESCE(team_one_score, 0)) AS team_one_total_score,
        SUM(COALESCE(team_two_score, 0)) AS team_two_total_score,
        COUNT(*) AS game_count
    FROM {match_games_fqn}
    GROUP BY match_id
)
SELECT
    m.match_id,
    m.match_sk,
    m.match_date,
    m.competition_category,
    m.match_status,
    m.winning_team_number,
    s1.team_one_id,
    s1.team_one_name,
    s2.team_two_id,
    s2.team_two_name,
    COALESCE(gs.team_one_total_score, 0) AS team_one_total_score,
    COALESCE(gs.team_two_total_score, 0) AS team_two_total_score,
    COALESCE(gs.game_count, 0) AS game_count,
    m.region_id,
    r.region_name,
    m.batch_id,
    b.batch_date
FROM {matches_fqn} m
LEFT JOIN side_one s1
    ON m.match_id = s1.match_id
LEFT JOIN side_two s2
    ON m.match_id = s2.match_id
LEFT JOIN game_scores gs
    ON m.match_id = gs.match_id
LEFT JOIN {regions_fqn} r
    ON m.region_id = r.region_id
LEFT JOIN {batches_fqn} b
    ON m.batch_id = b.batch_id
""".strip()


def build_vw_player_match_history(
    match_team_players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    match_teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    matches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    players_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    teams_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    regions_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    monthly_batches_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    """Return one row per player-match participation."""
    match_teams_index = {str(row["match_team_id"]): row for row in match_teams_rows}
    matches_index = {str(row["match_id"]): row for row in matches_rows}
    players_index = {str(row["player_id"]): row for row in players_rows}
    teams_index = {str(row["team_id"]): row for row in teams_rows if row.get("team_id") not in (None, "")}
    regions_index = {str(row["region_id"]): row for row in regions_rows if row.get("region_id") not in (None, "")}
    batches_index = {str(row["batch_id"]): row for row in monthly_batches_rows if row.get("batch_id") not in (None, "")}

    sides_by_match: dict[str, list[dict[str, Any]]] = {}
    for row in match_teams_rows:
        sides_by_match.setdefault(str(row["match_id"]), []).append(row)

    results: list[dict[str, Any]] = []
    for row in sorted(
        match_team_players_rows,
        key=lambda item: (str(item["player_id"]), str(item["match_id"])),
    ):
        match_team_row = match_teams_index[str(row["match_team_id"])]
        match_row = matches_index[str(row["match_id"])]
        player_row = players_index[str(row["player_id"])]
        team_row = teams_index.get(str(row["team_id"])) if row.get("team_id") else None
        region_row = regions_index.get(str(match_row["region_id"])) if match_row.get("region_id") else None
        batch_row = batches_index.get(str(match_row["batch_id"])) if match_row.get("batch_id") else None
        opponent_team_id = _resolve_opponent_team_id(
            sides_by_match.get(str(row["match_id"]), []),
            current_match_team_id=str(row["match_team_id"]),
        )
        opponent_team = teams_index.get(opponent_team_id) if opponent_team_id else None
        results.append(
            {
                "player_id": row["player_id"],
                "player_sk": row["player_sk"],
                "display_name": player_row.get("display_name"),
                "match_id": row["match_id"],
                "match_sk": row["match_sk"],
                "match_date": match_row.get("match_date"),
                "competition_category": match_row.get("competition_category"),
                "team_id": row.get("team_id"),
                "team_name": team_row.get("team_name") if team_row else None,
                "opponent_team_id": opponent_team_id,
                "opponent_team_name": opponent_team.get("team_name") if opponent_team else None,
                "result": _resolve_player_match_result(match_row.get("winning_team_number"), match_team_row.get("team_number")),
                "player_rating_at_match": row.get("player_rating_at_match"),
                "region_id": match_row.get("region_id"),
                "region_name": region_row.get("region_name") if region_row else None,
                "batch_id": match_row.get("batch_id"),
                "batch_date": batch_row.get("batch_date") if batch_row else None,
            }
        )
    return tuple(results)


def build_vw_player_match_history_sql(environment: ReleaseEnvironment) -> str:
    """Return SQL for the player-match-history convenience view."""
    match_team_players_fqn = _silver_table_fqn(environment, "match_team_players")
    match_teams_fqn = _silver_table_fqn(environment, "match_teams")
    matches_fqn = _silver_table_fqn(environment, "matches")
    players_fqn = _silver_table_fqn(environment, "players")
    teams_fqn = _silver_table_fqn(environment, "teams")
    regions_fqn = _silver_table_fqn(environment, "regions")
    batches_fqn = _silver_table_fqn(environment, "monthly_batches")
    return f"""
WITH opponent_teams AS (
    SELECT
        mt1.match_team_id,
        mt2.team_id AS opponent_team_id
    FROM {match_teams_fqn} mt1
    LEFT JOIN {match_teams_fqn} mt2
        ON mt1.match_id = mt2.match_id
       AND mt1.match_team_id <> mt2.match_team_id
)
SELECT
    mtp.player_id,
    mtp.player_sk,
    p.display_name,
    mtp.match_id,
    mtp.match_sk,
    m.match_date,
    m.competition_category,
    mtp.team_id,
    t.team_name,
    ot.opponent_team_id,
    ot_team.team_name AS opponent_team_name,
    CASE
        WHEN m.winning_team_number IS NULL OR mt.team_number IS NULL THEN NULL
        WHEN m.winning_team_number = mt.team_number THEN 'WIN'
        ELSE 'LOSS'
    END AS result,
    mtp.player_rating_at_match,
    m.region_id,
    r.region_name,
    m.batch_id,
    b.batch_date
FROM {match_team_players_fqn} mtp
INNER JOIN {match_teams_fqn} mt
    ON mtp.match_team_id = mt.match_team_id
INNER JOIN {matches_fqn} m
    ON mtp.match_id = m.match_id
LEFT JOIN {players_fqn} p
    ON mtp.player_id = p.player_id
LEFT JOIN {teams_fqn} t
    ON mtp.team_id = t.team_id
LEFT JOIN opponent_teams ot
    ON mtp.match_team_id = ot.match_team_id
LEFT JOIN {teams_fqn} ot_team
    ON ot.opponent_team_id = ot_team.team_id
LEFT JOIN {regions_fqn} r
    ON m.region_id = r.region_id
LEFT JOIN {batches_fqn} b
    ON m.batch_id = b.batch_id
""".strip()


def _aggregate_game_scores(
    match_games_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, int]:
    team_one_total = sum(int(row.get("team_one_score") or 0) for row in match_games_rows)
    team_two_total = sum(int(row.get("team_two_score") or 0) for row in match_games_rows)
    return {
        "team_one_total_score": team_one_total,
        "team_two_total_score": team_two_total,
    }


def _resolve_opponent_team_id(
    match_sides: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    current_match_team_id: str,
) -> str | None:
    for side in match_sides:
        if str(side["match_team_id"]) != current_match_team_id:
            team_id = side.get("team_id")
            return str(team_id) if team_id not in (None, "") else None
    return None


def _resolve_player_match_result(
    winning_team_number: Any,
    team_number: Any,
) -> str | None:
    if winning_team_number is None or team_number is None:
        return None
    return "WIN" if int(winning_team_number) == int(team_number) else "LOSS"


def _silver_table_fqn(environment: ReleaseEnvironment, table_name: str) -> str:
    return f"{environment.catalog}.{environment.silver_schema}.{table_name}"
