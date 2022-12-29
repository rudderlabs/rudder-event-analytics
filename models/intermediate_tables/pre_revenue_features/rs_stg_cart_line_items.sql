
with
    cte_seq as ({{ dbt_utils.generate_series(upper_bound=20) }}),

    cte_valid_arrays_c as (
        select {{ var("col_shopify_cart_create_user_id") }} as cart_token, {{ var('main_id') }},
        id
        from {{ ref("rs_stg_cart_create") }} c
        where is_valid_json_array({{ var("col_shopify_cart_create_products") }}) = true
    ),

    cte_valid_arrays_u as (
        select {{ var("col_shopify_cart_update_user_id") }} as cart_token, {{ var('main_id') }},
         id
        from {{ ref("rs_stg_cart_update") }} c
        where is_valid_json_array({{ var("col_shopify_cart_update_products") }}) = true
    ),
    cte_line_items_all as 
(select distinct
    s.*,
    dense_rank() over (
        partition by cart_token, product_id, id
        order by {{ var("col_shopify_cart_create_timestamp") }} desc
    ) as line_item_recency
from
    (
        select
            {{ var('main_id') }},
            {{ var("col_shopify_cart_create_user_id") }} as cart_token,
            id as event_id,
            {{ var("col_shopify_cart_create_timestamp") }},
            {% for json_element in var("var_product_elements") %}
            json_extract_path_text(
                json_extract_array_element_text(
                    {{ var("col_shopify_cart_create_products") }},
                    (seq.generated_number - 1)::int
                ),
                '{{json_element}}'
            ) as {{ json_element }}
            {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ ref("rs_stg_cart_create") }}, cte_seq seq
        where
            seq.generated_number
            < json_array_length({{ var("col_shopify_cart_create_products") }})
            and id in (select distinct id from cte_valid_arrays_c)

        union all

        select
            {{ var('main_id') }},
            {{ var("col_shopify_cart_update_user_id") }} as cart_token,
            id as event_id,
            {{ var("col_shopify_cart_update_timestamp") }},
            {% for json_element in var("var_product_elements") %}
            json_extract_path_text(
                json_extract_array_element_text(
                    {{ var("col_shopify_cart_update_products") }},
                    (seq.generated_number - 1)::int
                ),
                '{{json_element}}'
            ) as {{ json_element }}
            {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ ref("rs_stg_cart_update") }}, cte_seq seq
        where
            seq.generated_number
            <= json_array_length({{ var("col_shopify_cart_update_products") }})
            and id in (select distinct id from cte_valid_arrays_u)
    ) s)
    select * from cte_line_items_all where line_item_recency = 1
