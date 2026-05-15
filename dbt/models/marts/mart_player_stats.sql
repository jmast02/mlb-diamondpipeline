with events as (
    select * from {{ ref('stg_game_events') }}
    where is_complete = true
      and batter_id is not null
),

batting as (
    select
        batter_id                                                                           as player_id,
        batter_name                                                                         as player_name,
        count(*)                                                                            as plate_appearances,
        count(*) filter (where event_type in ('Single', 'Double', 'Triple', 'Home Run'))   as hits,
        count(*) filter (where event_type = 'Single')                                       as singles,
        count(*) filter (where event_type = 'Double')                                       as doubles,
        count(*) filter (where event_type = 'Triple')                                       as triples,
        count(*) filter (where event_type = 'Home Run')                                     as home_runs,
        coalesce(sum(rbi), 0)                                                               as rbi,
        count(*) filter (where event_type = 'Walk')                                         as walks,
        count(*) filter (where event_type = 'Strikeout')                                    as strikeouts,
        count(*) filter (where is_scoring_play = true)                                      as scoring_plays,
        count(distinct game_pk)                                                             as games
    from events
    group by 1, 2
),

with_rates as (
    select
        *,
        round(hits::numeric / nullif(plate_appearances, 0), 3)          as batting_avg,
        round(
            (hits + walks)::numeric / nullif(plate_appearances, 0), 3
        )                                                                as obp,
        round(
            (singles + 2 * doubles + 3 * triples + 4 * home_runs)::numeric
            / nullif(plate_appearances, 0), 3
        )                                                                as slg
    from batting
)

select
    *,
    round(obp + slg, 3) as ops
from with_rates
where plate_appearances >= 3
order by batting_avg desc
