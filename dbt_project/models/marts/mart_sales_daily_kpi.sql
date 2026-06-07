with sales as (

    select *
    from {{ ref('fct_sales') }}

)

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

    max(load_ts) as latest_load_ts,
    current_timestamp() as dbt_updated_at

from sales
group by order_date