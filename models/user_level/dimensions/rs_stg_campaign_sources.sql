{% if var('col_shopify_identifies_campaign_source') != '' %}
  {{config(enabled=True)}}
{% else %}
  {{config(enabled=False)}}
{% endif %}

select 
    {{ var('main_id')}}, 
    ({{ array_agg( var('col_shopify_identifies_campaign_source') )}}) as campaign_sources 
from {{ source('rudder', 'identifies') }}
where {{timebound( var('col_shopify_identifies_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}