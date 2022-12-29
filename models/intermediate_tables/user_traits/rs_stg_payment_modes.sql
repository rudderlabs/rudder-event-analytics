{% if var('col_shopify_identifies_payment_method') != '' %}
  {{config(enabled=True)}}
{% else %}
  {{config(enabled=False)}}
{% endif %}

select 
    {{ var('main_id')}}, 
    {{array_agg( var('col_shopify_identifies_payment_method') )}} as payment_modes 
from {{ ref('rs_stg_identifies') }}
where {{timebound( var('col_shopify_identifies_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}