with
    cte_latest_cart_token as (
        select
            {{ var("main_id") }},
            line_price,
            cart_token,
            product_id,
            first_value(cart_token) over (
                partition by {{ var("main_id") }}
                order by
                    {{ var("col_shopify_cart_create_timestamp") }} desc {{ frame_clause() }}
            ) as latest_cart_token
        from {{ ref('rs_stg_cart_line_items') }}
    )
select
    {{ var("main_id") }},
    'last_cart_value_in_dollars' as feature_name,
    sum(line_price)::varchar as feature_value,
    'numeric' as feature_type
from cte_latest_cart_token
where cart_token = latest_cart_token
group by 1, 2
union all
select {{var("main_id")}},
'last_cart_items_list' as feature_name, 
{{array_agg('product_id')}} as feature_value ,
'string' as feature_type
from cte_latest_cart_token where cart_token = latest_cart_token
group by 1,2