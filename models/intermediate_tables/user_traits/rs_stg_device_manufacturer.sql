{% if var('col_shopify_identifies_device_manufacturer') != '' %}
  {{config(enabled=True)}}
{% else %}
  {{config(enabled=False)}}
{% endif %}

select 
    distinct {{ var('main_id')}},
    first_value({{ var('col_shopify_identifies_device_manufacturer')}}) over(
        partition by {{ var('main_id')}} 
        order by case when {{ var('col_shopify_identifies_device_manufacturer')}} is not null and {{ var('col_shopify_identifies_device_manufacturer')}} != '' then 2 else 1 end desc, 
        {{ var('col_shopify_identifies_timestamp')}} desc {{frame_clause()}}
    ) as device_manufacturer
from {{ ref('rs_stg_identifies')}} 
where {{timebound( var('col_shopify_identifies_timestamp'))}} and {{ var('main_id')}} is not null