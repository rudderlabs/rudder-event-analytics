select
    y.{{ var("main_id") }},
    'last_cart_status' as feature_name,
    coalesce(o.fulfillment_status, o.financial_status, 'abandoned') as feature_value,
    'string' as feature_type
from
    (
        select
            {{ var("main_id") }},
            cart_token,
            {{ var("col_shopify_cart_create_timestamp") }},
            dense_rank() over (
                partition by {{ var("main_id") }}
                order by {{ var("col_shopify_cart_create_timestamp") }} desc
            ) as feature_rank
        from
            (
                select
                    {{ var("main_id") }},
                    token as cart_token,
                    i.{{ var("col_shopify_cart_create_timestamp") }}
                from {{ ref("rs_stg_cart_create") }} i
                where {{ var("col_shopify_cart_create_products") }} is not null

                union all

                select
                    {{ var("main_id") }},
                    token as cart_token,
                    i.{{ var("col_shopify_cart_create_timestamp") }}
                from {{ ref("rs_stg_cart_update") }} i
                where {{ var("col_shopify_cart_update_products") }} is not null
            ) a

    ) y
left outer join
    {{ ref("rs_stg_order_created") }} o
    on y.cart_token = o.{{ var("col_shopify_orders_cart_id") }}
where feature_rank = 1
