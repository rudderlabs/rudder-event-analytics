with cte_id_stitched_pages as (

    {{id_stitch( var('tbl_shopify_pages'), [ var('col_shopify_pages_user_id') ], var('col_shopify_pages_timestamp'))}}
    
)

select * from cte_id_stitched_pages