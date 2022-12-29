select 
    {{ var('main_id') }},
    sum({{ var('col_shopify_orders_cancelled_total_price') }}:: real) as total_refund
from {{ ref('rs_stg_orders_cancelled') }}
where {{timebound(var('col_shopify_orders_cancelled_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}