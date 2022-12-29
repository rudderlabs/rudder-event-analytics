with cte_id_stitched_tracks as (

    {{id_stitch( var('tbl_shopify_tracks'), [ var('col_shopify_tracks_user_id') ], var('col_shopify_tracks_timestamp'))}}
    
)

select * from cte_id_stitched_tracks