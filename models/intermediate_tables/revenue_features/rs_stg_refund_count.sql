select 
    {{ var('main_id') }}, 
     count(*) as refund_count
from {{ ref('rs_stg_orders_cancelled') }}
where {{timebound( var('col_shopify_orders_cancelled_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}