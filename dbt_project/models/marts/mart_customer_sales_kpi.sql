with sales as (

    select *
    from {{ ref('fct_sales') }}

)

select
    customer_id,

    count(*) as total_orders,
    count_if(is_completed_order) as completed_orders,
    count_if(is_failed_or_reversed_order) as failed_or_reversed_orders,

    min(order_date) as first_order_date,
    max(order_date) as latest_order_date,

    sum(case when is_completed_order then net_amount else 0 end) as completed_net_sales_amount,
    avg(case when is_completed_order then net_amount end) as avg_completed_order_amount,

    current_timestamp() as dbt_updated_at

from sales
group by customer_id