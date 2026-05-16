-- raw_schedule accumulates rows across days (append semantics).
-- Deduplicate by game_pk, preferring the most advanced status per game.
with source as (
    select
        game_pk::bigint       as game_pk,
        game_date::timestamp  as game_date,
        status                as game_status,
        home_team_id::integer as home_team_id,
        home_team             as home_team_name,
        away_team_id::integer as away_team_id,
        away_team             as away_team_name,
        home_score::integer   as home_score,
        away_score::integer   as away_score,
        venue                 as venue_name,
        row_number() over (
            partition by game_pk::bigint
            order by
                case status
                    when 'Final'       then 1
                    when 'Game Over'   then 1
                    when 'In Progress' then 2
                    when 'Pre-Game'    then 3
                    when 'Warmup'      then 3
                    else                    4
                end
        ) as rn
    from {{ source('raw', 'raw_schedule') }}
)

select
    game_pk, game_date, game_status,
    home_team_id, home_team_name,
    away_team_id, away_team_name,
    home_score, away_score, venue_name
from source
where rn = 1
