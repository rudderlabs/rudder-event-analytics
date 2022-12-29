select {{ var('main_id') }},
'total_products_added' as feature_name,
{{array_agg('product_id')}} as feature_value,
'array' as feature_type
from {{ ref('rs_stg_cart_line_items') }}
group by 1,2
union all
{% for lookback_days in var('lookback_days') %}
select {{ var('main_id') }},
concat(concat('products_added_in_past_', {{lookback_days}}::varchar), '_days') as feature_name,
{{array_agg('product_id')}} as feature_value,
'array' as feature_type
from {{ ref('rs_stg_cart_line_items') }}
where 
datediff(day, date({{ var('col_shopify_cart_create_timestamp') }}), date({{get_end_timestamp()}})) <= {{lookback_days}}
group by 1,2
{% if not loop.last %}
union all
{% endif %}
{% endfor %}