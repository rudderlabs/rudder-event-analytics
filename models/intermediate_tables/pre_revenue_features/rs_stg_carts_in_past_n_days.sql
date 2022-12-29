with cte_carts_in_past_n_days as (
    {% for lookback_days in var('lookback_days') %}
        select {{ var('main_id') }},
        count(distinct {{ var('col_shopify_cart_create_products') }}) as carts_in_past_n_days,
        {{lookback_days}} as n_value
        From (
            Select {{ var('main_id') }}, {{ var('col_shopify_cart_create_products') }}, {{ var('col_shopify_cart_create_timestamp') }} 
            from {{ ref('rs_stg_cart_create') }}
            where {{ var('col_shopify_cart_create_products') }} is not null 

            union all 

            Select {{ var('main_id') }}, {{ var('col_shopify_cart_update_products') }}, {{ var('col_shopify_cart_update_timestamp') }} 
            from {{ ref('rs_stg_cart_update') }}
            where {{ var('col_shopify_cart_update_products') }} is not null 
        )
        where datediff(day, date({{ var('col_shopify_cart_create_timestamp') }}), date({{get_end_timestamp()}})) <= {{lookback_days}} and {{timebound( var('col_shopify_cart_create_timestamp'))}} and {{ var('main_id')}} is not null
        group by {{ var('main_id') }}
    {% if not loop.last %} union {% endif %}
    {% endfor %}
)

{% for lookback_days in var('lookback_days') %}
select {{ var('main_id') }}, 
    'carts_in_past_{{lookback_days}}_days' as feature_name, 
    carts_in_past_n_days as feature_value , 
    'numeric' as feature_type
from cte_carts_in_past_n_days where n_value = {{lookback_days}}
{% if not loop.last %} union {% endif %}
{% endfor %}