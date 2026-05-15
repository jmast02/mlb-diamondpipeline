with source as (
    select * from {{ source('raw', 'raw_schedule') }}
)

select
    game_pk::bigint              as game_pk,
    game_date::timestamp         as game_date,
    status                       as game_status,
    home_team_id::integer        as home_team_id,
    home_team                    as home_team_name,
    away_team_id::integer        as away_team_id,
    away_team                    as away_team_name,
    home_score::integer          as home_score,
    away_score::integer          as away_score,
    venue                        as venue_name
from source
