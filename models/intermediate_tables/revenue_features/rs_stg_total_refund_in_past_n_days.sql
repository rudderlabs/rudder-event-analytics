with
    cte_amt_spent_in_past_n_days as (
        {% for lookback_days in var('lookback_days') %}
        select
            {{ var('main_id') }},
            sum(
                {{ var('col_shopify_orders_cancelled_total_price') }}::real
            ) as total_refund_in_past_n_days,
            {{ lookback_days }} as n_value
        from {{ ref('rs_stg_orders_cancelled') }}
        where
            datediff(
                day,
                date({{ var('col_shopify_orders_cancelled_timestamp') }}),
                date({{ get_end_timestamp() }})
            )
            <= {{ lookback_days }}
            and {{ timebound(var('col_shopify_orders_cancelled_timestamp')) }}
            and {{ var('main_id') }} is not null
        group by {{ var('main_id') }}
        {% if not loop.last %}
        union
        {% endif %}
        {% endfor %}
    )

{% for lookback_days in var('lookback_days') %}
select
    {{ var('main_id') }},
    'total_refund_in_past_{{lookback_days}}_days' as feature_name,
    total_refund_in_past_n_days as feature_value
from cte_amt_spent_in_past_n_days
where n_value = {{ lookback_days }}
{% if not loop.last %}
union
{% endif %}
{% endfor %}
