-- Full season pitching leaderboard using official MLB stats.
-- Classifies players as Starters (≥50% of appearances as starts) or Relievers.
with pitchers as (
    select
        *,
        case
            when games > 0 and games_started::numeric / games >= 0.5 then 'Starter'
            else 'Reliever'
        end as role
    from {{ ref('stg_pitching_stats') }}
    where games > 0
)

select * from pitchers
order by
    case role when 'Starter' then 1 else 2 end,
    era asc nulls last
