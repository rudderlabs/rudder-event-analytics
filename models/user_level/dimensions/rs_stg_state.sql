{% if var('col_shopify_identifies_state') != '' %}
  {{config(enabled=True)}}
{% else %}
  {{config(enabled=False)}}
{% endif %}

with cte_user_max_time as (

    select {{ var('main_id') }}, 
        max({{ var('col_shopify_identifies_timestamp') }}) as recent_ts 
    from {{ source('rudder', 'identifies') }}
    group by 1
    
)

select a.{{ var('main_id') }}, 
    max({{ var('col_shopify_identifies_state') }}) as state 
from {{ source('rudder', 'identifies') }} a 
left join cte_user_max_time b 
on a.{{ var('main_id') }} = b.{{ var('main_id') }}
group by 1