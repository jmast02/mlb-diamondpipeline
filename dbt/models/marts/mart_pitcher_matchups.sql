with events as (
    select * from {{ ref('stg_game_events') }}
    where is_complete = true
      and batter_id  is not null
      and pitcher_id is not null
),

matchups as (
    select
        pitcher_id,
        pitcher_name,
        batter_id,
        batter_name,
        count(*)                                                                            as plate_appearances,
        count(*) filter (where event_type in ('Single', 'Double', 'Triple', 'Home Run'))   as hits,
        count(*) filter (where event_type = 'Home Run')                                     as home_runs,
        count(*) filter (where event_type = 'Strikeout')                                    as strikeouts,
        count(*) filter (where event_type = 'Walk')                                         as walks,
        coalesce(sum(rbi), 0)                                                               as rbi,
        round(
            count(*) filter (where event_type in ('Single', 'Double', 'Triple', 'Home Run'))::numeric
            / nullif(count(*), 0),
            3
        )                                                                                   as batting_avg_vs
    from events
    group by 1, 2, 3, 4
)

select * from matchups
where plate_appearances >= 2
order by plate_appearances desc, batting_avg_vs desc
