{{
    config(
        materialized='incremental',
        unique_key='event_id',
    )
}}

with source as (
    select
        game_pk::text || '_' || at_bat_index::text  as event_id,
        game_pk                                      as game_pk,
        at_bat_index::integer                        as at_bat_index,
        inning::integer                              as inning,
        half_inning                                  as half_inning,
        start_time::timestamp                        as start_time,
        end_time::timestamp                          as end_time,
        is_complete                                  as is_complete,
        is_scoring                                   as is_scoring_play,
        event_type                                   as event_type,
        description                                  as description,
        rbi::integer                                 as rbi,
        home_score::integer                          as home_score,
        away_score::integer                          as away_score,
        batter_id::integer                           as batter_id,
        batter_name                                  as batter_name,
        pitcher_id::integer                          as pitcher_id,
        pitcher_name                                 as pitcher_name
    from {{ source('raw', 'raw_game_events') }}
    {% if is_incremental() %}
    where (game_pk::text || '_' || at_bat_index::text) not in (
        select event_id from {{ this }}
    )
    {% endif %}
)

-- Deduplicate: if producer/consumer runs multiple times, pick one row per event
select distinct on (event_id) *
from source
order by event_id
