select 
    distinct {{ var('main_id') }},
    first_value({{ var('col_shopify_order_created_total_price') }}::real) over(
        partition by {{ var('main_id') }} 
        order by case when {{ var('col_shopify_order_created_total_price') }} is not null then 2 else 1 end desc, 
        {{var('col_shopify_order_created_timestamp')}} desc {{frame_clause()}}
    ) as last_transaction_value
from {{ ref('rs_stg_order_created') }}
where {{timebound( var('col_shopify_order_created_timestamp'))}} and {{ var('main_id')}} is not null