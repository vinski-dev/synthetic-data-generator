with sales as (

    select *
    from {{ ref('fct_sales') }}

),

category_kpi as (

    select
        product_category,

        count(*) as total_orders,
        count_if(is_completed_order) as completed_orders,
        count_if(is_failed_or_reversed_order) as failed_or_reversed_orders,

        count(distinct product_id) as distinct_products,
        count(distinct customer_id) as unique_customers,

        sum(gross_amount) as gross_sales_amount,
        sum(discount_amount) as total_discount_amount,
        sum(net_amount) as net_sales_amount,

        sum(case when is_completed_order then net_amount else 0 end) as completed_net_sales_amount,
        avg(case when is_completed_order then net_amount end) as avg_completed_order_amount,

        max(load_ts) as latest_load_ts,
        current_timestamp() as dbt_updated_at

    from sales
    group by product_category

)

select *
from category_kpi