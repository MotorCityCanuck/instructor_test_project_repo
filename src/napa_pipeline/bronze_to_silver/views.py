"""Convenience-view builders for the Bronze-to-Silver pipeline."""

from __future__ import annotations

from typing import Any


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
