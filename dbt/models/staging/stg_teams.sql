with source as (
    select * from {{ source('raw', 'raw_teams') }}
)

select
    team_id::integer      as team_id,
    team_name             as team_name,
    abbreviation          as abbreviation,
    team_code             as team_code,
    division_id::integer  as division_id,
    division_name         as division_name,
    league_id::integer    as league_id,
    league_name           as league_name,
    venue_id::integer     as venue_id,
    venue_name            as venue_name,
    season::integer       as season
from source
