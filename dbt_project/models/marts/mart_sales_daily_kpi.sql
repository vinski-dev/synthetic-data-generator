with sales as (

    select *
    from {{ ref('fct_sales') }}

),

daily_sales as (

    select
        order_date,

        count(*) as total_orders,
        count_if(is_completed_order) as completed_orders,
        count_if(order_status = 'PENDING') as pending_orders,
        count_if(order_status = 'CANCELLED') as cancelled_orders,
        count_if(order_status = 'REFUNDED') as refunded_orders,

        count(distinct customer_id) as unique_customers,

        sum(gross_amount) as gross_sales_amount,
        sum(discount_amount) as total_discount_amount,
        sum(net_amount) as net_sales_amount,

        avg(net_amount) as avg_order_value,

        case
            when sum(gross_amount) > 0 then sum(discount_amount) / sum(gross_amount)
            else 0
        end as discount_rate,

        max(load_ts) as latest_load_ts

    from sales
    group by order_date

),

date_dim as (

    select *
    from {{ ref('dim_date') }}

)

select
    daily_sales.order_date,

    date_dim.year_number,
    date_dim.quarter_number,
    date_dim.month_number,
    date_dim.month_name,
    date_dim.week_number,
    date_dim.day_of_week_name,
    date_dim.is_weekend,

    daily_sales.total_orders,
    daily_sales.completed_orders,
    daily_sales.pending_orders,
    daily_sales.cancelled_orders,
    daily_sales.refunded_orders,

    daily_sales.unique_customers,

    daily_sales.gross_sales_amount,
    daily_sales.total_discount_amount,
    daily_sales.net_sales_amount,
    daily_sales.avg_order_value,
    daily_sales.discount_rate,

    daily_sales.latest_load_ts,
    current_timestamp() as dbt_updated_at

from daily_sales
left join date_dim
    on daily_sales.order_date = date_dim.date_day