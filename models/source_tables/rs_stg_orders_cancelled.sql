with cte_id_stitched_orders_cancelled as (

    {{id_stitch( var('tbl_shopify_orders_cancelled'), [ var('col_shopify_orders_cancelled_user_id') ], var('col_shopify_orders_cancelled_timestamp'))}}
    
)

select * from cte_id_stitched_orders_cancelled