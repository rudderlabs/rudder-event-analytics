with cte_id_stitched_pages as (

    {{id_stitch( var('tbl_rudder_pages'), 
    [ var('col_rudder_pages_user_id') ], 
    var('col_rudder_pages_timestamp'))}}
    
)

select * from cte_id_stitched_pages