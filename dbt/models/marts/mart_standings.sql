with standings as (
    select * from {{ ref('stg_standings') }}
),

ranked as (
    select
        *,
        rank() over (
            partition by division_name
            order by wins desc, losses asc
        ) as division_rank,
        rank() over (
            partition by league_name
            order by wins desc, losses asc
        ) as league_rank
    from standings
)

select * from ranked
order by league_name, division_name, division_rank
