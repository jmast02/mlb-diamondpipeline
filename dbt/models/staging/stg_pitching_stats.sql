-- Official MLB season pitching stats. ERA/WHIP are strings — cast safely.
-- innings_pitched kept as text (baseball notation: "62.1" = 62⅓ innings).
-- Deduplicates by player_id keeping the row with the most games pitched.
with source as (
    select
        player_id::integer           as player_id,
        player_name                  as player_name,
        team_id::integer             as team_id,
        team_name                    as team_name,
        games::integer               as games,
        games_started::integer       as games_started,
        wins::integer                as wins,
        losses::integer              as losses,
        saves::integer               as saves,
        innings_pitched              as innings_pitched,
        hits_allowed::integer        as hits_allowed,
        earned_runs::integer         as earned_runs,
        walks_allowed::integer       as walks_allowed,
        strikeouts::integer          as strikeouts,
        home_runs_allowed::integer   as home_runs_allowed,
        case when era    ~ '^[0-9.]+$' then era::numeric(6,2)    end as era,
        case when whip   ~ '^[0-9.]+$' then whip::numeric(5,3)   end as whip,
        case when k_per_9 ~ '^[0-9.]+$' then k_per_9::numeric(5,2) end as k_per_9,
        case when bb_per_9 ~ '^[0-9.]+$' then bb_per_9::numeric(5,2) end as bb_per_9,
        season::integer              as season
    from {{ source('raw', 'raw_pitching_stats') }}
    where player_id is not null
)

select distinct on (player_id) *
from source
order by player_id, games desc
