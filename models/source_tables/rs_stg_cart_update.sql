with cte_id_stitched_cart_update as (

    {{id_stitch( var('tbl_shopify_cart_update'), [var('col_shopify_cart_update_user_id')], var('col_shopify_cart_update_timestamp'))}}
)

select * from cte_id_stitched_cart_update 