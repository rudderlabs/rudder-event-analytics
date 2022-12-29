with cte_active_days_in_past_n_days as (

    {% for lookback_days in var('lookback_days') %}
        select {{ var('main_id') }},
        count(distinct date({{ var('col_shopify_tracks_timestamp') }})) as active_days_in_past_n_days,
        {{lookback_days}} as n_value
        from 
        (select {{var('main_id')}},{{ var('col_shopify_tracks_timestamp') }} from  {{ ref('rs_stg_tracks') }} 
        where datediff(day, date({{ var('col_shopify_tracks_timestamp') }}), date({{get_end_timestamp()}})) <= {{lookback_days}} and {{timebound( var('col_shopify_tracks_timestamp'))}} and {{ var('main_id')}} is not null
        union all 
        select {{var('main_id')}},{{ var('col_shopify_pages_timestamp') }} from  {{ ref('rs_stg_pages') }} 
        where datediff(day, date({{ var('col_shopify_pages_timestamp') }}), date({{get_end_timestamp()}})) <= {{lookback_days}} and {{timebound( var('col_shopify_pages_timestamp'))}} and {{ var('main_id')}} is not null)
        group by {{ var('main_id') }}
    {% if not loop.last %} union {% endif %}
    {% endfor %}
)

{% for lookback_days in var('lookback_days') %}
select {{ var('main_id') }}, 
    'active_days_in_past_{{lookback_days}}_days' as feature_name, cast(active_days_in_past_n_days as varchar) as feature_value 
from cte_active_days_in_past_n_days 
where n_value = {{lookback_days}}
{% if not loop.last %} union {% endif %}
{% endfor %}