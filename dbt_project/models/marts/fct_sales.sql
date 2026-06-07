{{
    config(
        materialized='incremental',
        unique_key='sales_event_key',
        incremental_strategy='merge',
        snowflake_warehouse='WH_DBT_MARTS',
        on_schema_change='sync_all_columns'
    )
}}

with sales as (

    select *
    from {{ ref('int_sales_enriched') }}

    {% if is_incremental() %}
        where load_ts >= (
            select dateadd(
                hour,
                -1,
                coalesce(max(load_ts), '1900-01-01'::timestamp_ltz)
            )
            from {{ this }}
        )
    {% endif %}

)

select
    sales_event_key,
    order_id,
    customer_id,
    product_id,
    product_category,

    order_timestamp,
    order_date,
    order_month,

    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    net_amount,
    discount_rate,

    payment_method,
    order_status,
    is_completed_order,
    is_failed_or_reversed_order,

    batch_id,
    source_file_name,
    generated_at_utc,
    load_ts,

    current_timestamp() as dbt_updated_at

from sales