{% if var('col_shopify_identifies_card_network') != '' %}
  {{config(enabled=True)}}
{% else %}
  {{config(enabled=False)}}
{% endif %}

with cte_user_max_time_currency as (

    select {{ var('main_id') }}, 
        max({{ var('col_shopify_identifies_timestamp') }}) as recent_ts 
    from {{ ref('rs_stg_identifies') }} 
    group by 1

)

select a.{{ var('main_id') }},  
    max({{ var('col_shopify_identifies_card_network') }}) as card_network 
from {{ ref('rs_stg_identifies') }} a
left join cte_user_max_time_currency b 
on a.{{ var('main_id') }} = b.{{ var('main_id') }}
group by 1