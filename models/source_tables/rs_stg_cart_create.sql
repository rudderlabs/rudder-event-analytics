with cte_id_stitched_cart_create as (

    {{id_stitch( var('tbl_shopify_cart_create'), [var('col_shopify_cart_create_user_id')], var('col_shopify_cart_create_timestamp'))}}
)

select * from cte_id_stitched_cart_create