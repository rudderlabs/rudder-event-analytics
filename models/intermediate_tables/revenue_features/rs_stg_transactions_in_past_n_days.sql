{{ config( 
    materialized = 'table'
   ) 
}} 
with
    cte_amt_spent_in_past_n_days as (
        {% for lookback_days in var('lookback_days') %}
        select
            a.{{ var('main_id') }},
            sum(
                a.{{ var('col_shopify_order_created_total_price') }}::real
            ) as gross_amt_spent_in_past_n_days,
            sum(a.{{ var('col_shopify_order_created_total_price') }}::real) - coalesce(
                sum(b.{{ var('col_shopify_orders_cancelled_total_price') }}::real), 0
            ) as net_amt_spent_in_past_n_days,
            count(*)::real as transactions_in_past_n_days,
            {{ lookback_days }} as n_value
        from {{ ref('rs_stg_order_created') }} a
        left join
            (select * from {{ ref('rs_stg_orders_cancelled') }}) b
            on a.{{ var('col_shopify_order_created_user_id') }}
            = b.{{ var('col_shopify_orders_cancelled_user_id') }}
            and a.{{ var('col_shopify_orders_cancelled_order_number') }}
            = b.{{ var('col_shopify_orders_cancelled_order_number') }}
        where
            datediff(
                day,
                date(a.{{ var('col_shopify_order_created_timestamp') }}),
                date({{ get_end_timestamp() }})
            )
            <= {{ lookback_days }}
            and a.{{ timebound(var('col_shopify_order_created_timestamp')) }}
            and a.{{ var('main_id') }} is not null
        group by a.{{ var('main_id') }}
        {% if not loop.last %}
        union all
        {% endif %}
        {% endfor %}
    ),
    cte_amt_spent_alltime as (
        select
            a.{{ var('main_id') }},
            sum(
                a.{{ var('col_shopify_order_created_total_price') }}::real
            ) as gross_amt_spent_in_past,
            sum(a.{{ var('col_shopify_order_created_total_price') }}::real) - coalesce(
                sum(b.{{ var('col_shopify_orders_cancelled_total_price') }}::real), 0
            ) as net_amt_spent_in_past,
            count(*)::real as total_transactions
        from {{ ref('rs_stg_order_created') }} a
        left join
            (select * from {{ ref('rs_stg_orders_cancelled') }}) b
            on a.{{ var('col_shopify_order_created_user_id') }}
            = b.{{ var('col_shopify_orders_cancelled_user_id') }}
            and a.{{ var('col_shopify_orders_cancelled_order_number') }}
            = b.{{ var('col_shopify_orders_cancelled_order_number') }}
        where
            a.{{ timebound(var('col_shopify_order_created_timestamp')) }}
            and a.{{ var('main_id') }} is not null
        group by a.{{ var('main_id') }}
    )

{% for lookback_days in var('lookback_days') %}
select
    {{ var('main_id') }},
    'gross_amt_spent_in_past_{{lookback_days}}_days' as feature_name,
    cast(gross_amt_spent_in_past_n_days as varchar) as feature_value
from cte_amt_spent_in_past_n_days
where n_value = {{ lookback_days }}
union all
select
    {{ var('main_id') }},
    'net_amt_spent_in_past_{{lookback_days}}_days' as feature_name,
    cast(net_amt_spent_in_past_n_days as varchar) as feature_value
from cte_amt_spent_in_past_n_days
where n_value = {{ lookback_days }}
union all
select
    {{ var('main_id') }},
    'transactions_in_past_{{lookback_days}}_days' as feature_name,
    cast(transactions_in_past_n_days as varchar) as feature_value
from cte_amt_spent_in_past_n_days
where n_value = {{ lookback_days }}
union all
{% endfor %}
select
    {{ var('main_id') }},
    'gross_amt_spent_in_past' as feature_name,
    cast(gross_amt_spent_in_past as varchar) as feature_value
from cte_amt_spent_alltime
union all
select
    {{ var('main_id') }},
    'net_amt_spent_in_past' as feature_name,
    cast(net_amt_spent_in_past as varchar) as feature_value
from cte_amt_spent_alltime
union all 
select
    {{ var('main_id') }},
    'total_transactions' as feature_name,
    cast(total_transactions as varchar) as feature_value
from cte_amt_spent_alltime