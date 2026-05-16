-- Official MLB season hitting stats. String rate stats (avg/obp/slg/ops) are cast safely.
-- Deduplicates by player_id keeping the row with the most games (handles mid-season trades).
with source as (
    select
        player_id::integer          as player_id,
        player_name                 as player_name,
        team_id::integer            as team_id,
        team_name                   as team_name,
        games::integer              as games,
        at_bats::integer            as at_bats,
        plate_appearances::integer  as plate_appearances,
        hits::integer               as hits,
        doubles::integer            as doubles,
        triples::integer            as triples,
        home_runs::integer          as home_runs,
        rbi::integer                as rbi,
        walks::integer              as walks,
        strikeouts::integer         as strikeouts,
        stolen_bases::integer       as stolen_bases,
        case when batting_avg ~ '^[0-9.]+$' then batting_avg::numeric(5,3) end as batting_avg,
        case when obp         ~ '^[0-9.]+$' then obp::numeric(5,3)         end as obp,
        case when slg         ~ '^[0-9.]+$' then slg::numeric(5,3)         end as slg,
        case when ops         ~ '^[0-9.]+$' then ops::numeric(5,3)         end as ops,
        season::integer             as season
    from {{ source('raw', 'raw_hitting_stats') }}
    where player_id is not null
)

select distinct on (player_id) *
from source
order by player_id, games desc
