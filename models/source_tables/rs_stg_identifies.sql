with cte_id_stitched_identifies as (

    {{id_stitch( var('tbl_rudder_identifies'), 
    [var('col_rudder_identifies_user_id'), 
    var('col_rudder_identifies_email')], 
    var('col_rudder_identifies_timestamp'))}}
)

select * from cte_id_stitched_identifies 

-- ToDo: Add identifier columns as list variable in project.yml