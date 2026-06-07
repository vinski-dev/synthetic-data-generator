with sales as (

    select *
    from {{ ref('stg_sales') }}

),

enriched as (

    select
        sales_event_key,
        order_id,
        customer_id,
        product_id,
        product_category,

        order_timestamp,
        order_date,
        date_trunc('month', order_date)::date as order_month,

        quantity,
        unit_price,
        gross_amount,
        discount_amount,
        net_amount,

        case
            when gross_amount > 0 then discount_amount / gross_amount
            else 0
        end as discount_rate,

        payment_method,
        order_status,

        case
            when order_status = 'COMPLETED' then true
            else false
        end as is_completed_order,

        case
            when order_status in ('CANCELLED', 'REFUNDED') then true
            else false
        end as is_failed_or_reversed_order,

        batch_id,
        generated_at_utc,
        source_file_name,
        load_ts

    from sales

)

select *
from enriched