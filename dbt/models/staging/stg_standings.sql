with source as (
    select * from {{ source('raw', 'raw_standings') }}
)

select
    team_id::integer          as team_id,
    team_name                 as team_name,
    division_id::integer      as division_id,
    division                  as division_name,
    league_id::integer        as league_id,
    league                    as league_name,
    wins::integer             as wins,
    losses::integer           as losses,
    win_pct::numeric(4, 3)   as win_pct,
    games_back                as games_back,
    games_played::integer     as games_played,
    season::integer           as season
from source
