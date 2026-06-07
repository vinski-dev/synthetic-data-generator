with raw_files as (

    select
        source_file_name,
        count(*) as raw_row_count,
        min(load_ts) as first_loaded_at,
        max(load_ts) as last_loaded_at
    from {{ source('raw', 'sales_raw') }}
    group by source_file_name

),

fact_files as (

    select
        source_file_name,
        count(*) as fact_row_count,
        min(load_ts) as first_fact_loaded_at,
        max(load_ts) as last_fact_loaded_at
    from {{ ref('fct_sales') }}
    group by source_file_name

)

select
    raw_files.source_file_name,

    raw_files.raw_row_count,
    coalesce(fact_files.fact_row_count, 0) as fact_row_count,

    raw_files.raw_row_count - coalesce(fact_files.fact_row_count, 0) as row_count_difference,

    raw_files.first_loaded_at,
    raw_files.last_loaded_at,
    fact_files.first_fact_loaded_at,
    fact_files.last_fact_loaded_at,

    case
        when raw_files.raw_row_count = coalesce(fact_files.fact_row_count, 0)
            then 'MATCHED'
        else 'MISMATCHED'
    end as reconciliation_status,

    current_timestamp() as dbt_updated_at

from raw_files
left join fact_files
    on raw_files.source_file_name = fact_files.source_file_name