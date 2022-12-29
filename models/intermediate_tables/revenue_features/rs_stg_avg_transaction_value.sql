select 
    {{ var('main_id') }}, 
    avg({{ var('col_shopify_order_created_total_price') }}::real) as avg_transaction_value 
from {{ ref('rs_stg_order_created') }}
where {{timebound( var('col_shopify_order_created_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}