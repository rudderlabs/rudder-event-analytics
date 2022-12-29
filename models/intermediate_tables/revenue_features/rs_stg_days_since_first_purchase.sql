select 
    distinct {{ var('main_id') }},
    datediff(day, date(min({{ var('col_shopify_order_created_timestamp') }})),date({{get_end_timestamp()}})) as days_since_first_purchase
from {{ ref('rs_stg_order_created') }}
where {{timebound( var('col_shopify_order_created_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}