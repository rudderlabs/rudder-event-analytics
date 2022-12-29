select 
    {{ var('main_id')}}, 
    max(case when lower({{ var('col_shopify_identifies_device_type')}}) like '%pc' then 1 else 0 end) as is_active_on_website 
from {{ ref('rs_stg_identifies')}}
where {{timebound( var('col_shopify_identifies_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}