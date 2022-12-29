select 
    {{ var('main_id')}}, 
    datediff(day, date(max({{ var('col_shopify_tracks_timestamp') }})),date({{get_end_timestamp()}})) as days_since_last_seen
from
(select {{var('main_id')}},{{ var('col_shopify_tracks_timestamp') }} from  {{ ref('rs_stg_tracks') }} 
where {{timebound( var('col_shopify_tracks_timestamp'))}} and {{ var('main_id')}} is not null
union all 
select {{var('main_id')}},{{ var('col_shopify_pages_timestamp') }} from  {{ ref('rs_stg_pages') }} 
where {{timebound( var('col_shopify_pages_timestamp'))}} and {{ var('main_id')}} is not null)
group by {{ var('main_id')}}