select 
    {{ var('main_id') }},
    'total_carts' as feature_name,
    count(distinct {{ var('col_shopify_cart_create_products') }}) as feature_value,
    'numeric' as feature_type
From (
    Select {{ var('main_id') }}, {{ var('col_shopify_cart_create_products') }}, {{ var('col_shopify_cart_create_timestamp') }} 
    from {{ ref('rs_stg_cart_create') }}
    where {{ var('col_shopify_cart_create_products') }} is not null 

    union all 

    Select {{ var('main_id') }}, {{ var('col_shopify_cart_update_products') }}, {{ var('col_shopify_cart_update_timestamp') }} 
    from {{ ref('rs_stg_cart_update') }}
    where {{ var('col_shopify_cart_update_products') }} is not null 
)
where {{timebound( var('col_shopify_cart_create_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}