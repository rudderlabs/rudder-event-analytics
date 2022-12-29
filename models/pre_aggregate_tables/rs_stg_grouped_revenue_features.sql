select {{ var('main_id')}}, 
    'last_transaction_value' as feature_name,  
    cast(last_transaction_value as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_last_transaction_value')}}
union all
select {{ var('main_id')}}, 
    'total_refund' as feature_name,  
    cast(total_refund as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_total_refund')}}

union all

select {{ var('main_id')}}, 
    'refund_count' as feature_name,  
    cast(refund_count as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_refund_count')}}

union all

select {{ var('main_id')}}, 
    'days_since_last_purchase' as feature_name,  
    cast(days_since_last_purchase as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_days_since_last_purchase')}}

union all

select {{ var('main_id')}}, 
    'days_since_first_purchase' as feature_name,  
    cast(days_since_first_purchase as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_days_since_first_purchase')}}


union all

{% for lookback_days in var('lookback_days')%}
select {{ var('main_id')}}, 
    feature_name,  
    cast(feature_value as varchar) as feature_value,
    'numeric' as feature_type 
from {{ref('rs_stg_total_refund_in_past_n_days')}} 
where feature_name = 'total_refund_in_past_{{lookback_days}}_days'
{% if not loop.last %} union {% endif %}
{% endfor %}


union all

select {{ var('main_id')}}, 
    'has_credit_card' as feature_name, 
    cast(has_credit_card as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_has_credit_card')}}
union all 
select {{ var('main_id')}}, 
feature_name, 
cast(feature_value as varchar) as feature_value, 
feature_type 
from {{ref('rs_stg_items_purchased_ever')}}
union all
select   {{ var('main_id') }}, 'avg_units_per_transaction' as feature_name , 
cast(avg_units_per_transaction as varchar) as feature_value,
'numeric' as feature_type from {{ ref('rs_feat_transaction_based') }}
union all
select   {{ var('main_id') }}, 'highest_transaction_value' as feature_name ,
cast(highest_transaction_value as varchar) as feature_value,
'numeric' as feature_type from {{ ref('rs_feat_transaction_based') }}
union all
select   {{ var('main_id') }}, 'avg_transaction_value' as feature_name ,
cast(avg_transaction_value as varchar) as feature_value,
'numeric' as feature_type from {{ ref('rs_feat_transaction_based') }}
union all
select   {{ var('main_id') }}, 'median_transaction_value' as feature_name ,
cast(median_transaction_value as varchar) as feature_value,
'numeric' as feature_type from {{ ref('rs_feat_transaction_based') }}
union all
select   {{ var('main_id') }}, 'total_transactions' as feature_name ,
cast(total_transactions as varchar) as feature_value,
'numeric' as feature_type from {{ ref('rs_feat_transaction_based') }}
union all
select {{ var('main_id') }}, feature_name, cast(feature_value as varchar) as feature_value,
'numeric' as feature_type from {{ ref('rs_stg_transactions_in_past_n_days') }}