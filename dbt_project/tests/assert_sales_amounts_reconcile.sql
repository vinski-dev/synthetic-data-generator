select
    sales_event_key,
    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    net_amount
from {{ ref('fct_sales') }}
where abs(gross_amount - (quantity * unit_price)) > 0.01
   or abs(net_amount - greatest(gross_amount - discount_amount, 0)) > 0.01