with cte_id_stitched_tracks as (

    {{id_stitch( var('tbl_rudder_tracks'), 
    [ var('col_rudder_tracks_user_id') ],
     var('col_rudder_tracks_timestamp'))}}
    
)

select * from cte_id_stitched_tracks