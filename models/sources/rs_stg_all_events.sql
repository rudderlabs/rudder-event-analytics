{{
    config(
        materialized='ephemeral'
    )
}}
with
    cte_tracks as ({{ get_utm_params(var('tbl_rudder_tracks')) }}),
    cte_pages as ({{ get_utm_params(var('tbl_rudder_pages')) }}),
    cte_identifies as ({{ get_utm_params(var('tbl_rudder_identifies')) }})
select *, {{ get_channel('utm_source', 'utm_medium', 'referrer') }} as channel, {{get_device_type(var('col_screen_height'), var('col_screen_width'))}} as device_type
from cte_tracks
union all
select *, {{ get_channel('utm_source', 'utm_medium', 'referrer') }} as channel, {{get_device_type(var('col_screen_height'), var('col_screen_width'))}} as device_type
from cte_pages
union all
select *, {{ get_channel('utm_source', 'utm_medium', 'referrer') }} as channel, {{get_device_type(var('col_screen_height'), var('col_screen_width'))}} as device_type
from cte_identifies