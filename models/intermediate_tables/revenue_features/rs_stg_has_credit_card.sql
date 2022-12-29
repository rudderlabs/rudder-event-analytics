select 
    {{ var('main_id') }}, 
    max(case when lower({{ var('col_shopify_order_created_payment_method') }}) in {{var('card_types')}} then 1 else 0 end) as has_credit_card
from {{ ref('rs_stg_order_created') }}
where {{timebound( var('col_shopify_order_created_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}