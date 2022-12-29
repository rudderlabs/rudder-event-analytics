select 
    {{ var('main_id') }}, 
    avg({{ array_size (  var('col_shopify_order_created_products') )}}::real) as avg_units_per_transaction , 
    avg({{ var('col_shopify_order_created_total_price') }}::real) as avg_transaction_value,
    max({{ var('col_shopify_order_created_total_price') }}::real) as highest_transaction_value,
    median({{ var('col_shopify_order_created_total_price') }}::real) as median_transaction_value, 
    count(*) as total_transactions
from {{ ref('rs_stg_order_created') }}
where {{timebound( var('col_shopify_order_created_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}

