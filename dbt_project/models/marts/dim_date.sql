with date_spine as (

    select
        dateadd(
            day,
            seq4(),
            '2024-01-01'::date
        ) as date_day
    from table(generator(rowcount => 1500))

),

date_dimension as (

    select
        date_day,

        year(date_day) as year_number,
        quarter(date_day) as quarter_number,
        month(date_day) as month_number,
        monthname(date_day) as month_name,

        week(date_day) as week_number,
        day(date_day) as day_of_month,
        dayofweek(date_day) as day_of_week_number,
        dayname(date_day) as day_of_week_name,

        date_trunc('week', date_day)::date as week_start_date,
        date_trunc('month', date_day)::date as month_start_date,
        last_day(date_day, 'month') as month_end_date,

        date_trunc('quarter', date_day)::date as quarter_start_date,
        last_day(date_day, 'quarter') as quarter_end_date,

        date_trunc('year', date_day)::date as year_start_date,
        last_day(date_day, 'year') as year_end_date,

        case
            when dayofweek(date_day) in (0, 6) then true
            else false
        end as is_weekend,

        current_timestamp() as dbt_updated_at

    from date_spine
    where date_day <= dateadd(year, 2, current_date)

)

select *
from date_dimension