with cte_id_stitched_identifies as (

    {{id_stitch( var('tbl_shopify_identifies'), [var('col_shopify_identifies_user_id'), var('col_shopify_identifies_email')], var('col_shopify_identifies_timestamp'))}}
)

select * from cte_id_stitched_identifies 

-- ToDo: Add identifier columns as list variable in project.yml