with cte_id_stitched_order_created as (

    {{id_stitch( var('tbl_shopify_order_created'), [ var('col_shopify_order_created_user_id') ], var('col_shopify_order_created_timestamp'))}}
    
)

select * from cte_id_stitched_order_created