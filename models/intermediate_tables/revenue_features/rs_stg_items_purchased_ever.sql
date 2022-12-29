	
with  
cte_seq as ({{ dbt_utils.generate_series(upper_bound=20) }}),
cte_valid_arrays_c as (
	select token as order_token, id
	from {{ ref('rs_stg_order_created') }} c 
	where is_valid_json_array({{ var('col_shopify_order_created_products') }}) = true	
	),
cte_order_line_items as 		
	(Select distinct s.*, 
    dense_rank() over (partition by order_token, 
    sku, 
    id order by {{ var('col_shopify_order_created_timestamp') }} desc ) as line_item_recency
	From 	(
		    SELECT  
            {{ var('main_id') }},
            cart_token as cart_token
				, token as order_token
				, number as order_number
				, checkout_id as checkout_id
				, checkout_token as checkout_token
		    	, id as event_id
				, event_text as event_text
		    	, {{ var('col_shopify_order_created_timestamp') }}
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, 
                (seq.generated_number - 1)::int), 'id') AS id
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, 
                (seq.generated_number - 1)::int), '{{ var('var_sku_element_in_product_json') }}') AS sku
		    FROM {{ ref('rs_stg_order_created') }} , cte_seq seq 
		    WHERE seq.generated_number <= JSON_ARRAY_LENGTH(products)
				and id IN (Select distinct id from cte_valid_arrays_c) 
) s 
)
select {{ var('main_id') }}, 'items_purchased_ever' as feature_name, 
{{array_agg('sku')}} as feature_value, 'array' as feature_type from cte_order_line_items where line_item_recency = 1
group by 1
