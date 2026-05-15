with schedule as (
    select * from {{ ref('stg_schedule') }}
    where game_status in ('Final', 'Game Over')
      and home_score is not null
      and away_score is not null
),

with_outcome as (
    select
        game_pk,
        game_date,
        game_status,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,
        home_score,
        away_score,
        venue_name,
        case
            when home_score > away_score then home_team_name
            when away_score > home_score then away_team_name
            else 'Tie'
        end                              as winning_team,
        case
            when home_score > away_score then away_team_name
            when away_score > home_score then home_team_name
            else 'Tie'
        end                              as losing_team,
        abs(home_score - away_score)     as run_differential
    from schedule
)

select * from with_outcome
order by game_date desc
