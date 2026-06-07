with source as (

    select *
    from {{ source('raw', 'sales_raw') }}

),

renamed as (

    select
        md5(
            concat_ws(
                '|',
                coalesce(order_id::string, ''),
                coalesce(batch_id::string, ''),
                coalesce(source_file_name::string, '')
            )
        ) as sales_event_key,

        order_id::number as order_id,
        customer_id::number as customer_id,
        product_id::number as product_id,

        initcap(trim(product_category)) as product_category,

        order_timestamp::timestamp_ntz as order_timestamp,
        order_timestamp::date as order_date,

        quantity::number as quantity,
        unit_price::number(18, 2) as unit_price,
        gross_amount::number(18, 2) as gross_amount,
        discount_amount::number(18, 2) as discount_amount,
        net_amount::number(18, 2) as net_amount,

        initcap(trim(payment_method)) as payment_method,
        upper(trim(order_status)) as order_status,

        batch_id::string as batch_id,
        generated_at_utc::timestamp_tz as generated_at_utc,

        source_file_name::string as source_file_name,
        load_ts::timestamp_ltz as load_ts

    from source

),

deduped as (

    select *
    from renamed
    qualify row_number() over (
        partition by sales_event_key
        order by load_ts desc
    ) = 1

)

select *
from deduped
where order_id is not null