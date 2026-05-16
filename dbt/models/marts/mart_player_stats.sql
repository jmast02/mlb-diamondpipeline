-- Full season batting leaderboard using official MLB stats.
-- Replaces the previous version that computed stats from play-by-play events.
select
    player_id,
    player_name,
    team_name,
    games,
    plate_appearances,
    at_bats,
    hits,
    doubles,
    triples,
    home_runs,
    rbi,
    walks,
    strikeouts,
    stolen_bases,
    batting_avg,
    obp,
    slg,
    ops,
    season
from {{ ref('stg_hitting_stats') }}
where plate_appearances >= 10
order by ops desc nulls last
