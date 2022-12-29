select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value, 
    feature_type 
from {{ref('rs_stg_carts_in_past_n_days')}}

union all

select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value, 
    feature_type 
from {{ref('rs_stg_days_since_last_cart_add')}}

union all 
select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value, 
    feature_type  from {{ ref('rs_stg_last_cart_status') }}
union all 
select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value, 
    feature_type  from {{ ref('rs_stg_last_cart_value_in_dollars') }}
union all 
select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value, 
    feature_type  from {{ ref('rs_stg_products_added_in_past_n_days') }}
union all 
select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value, 
    feature_type  from {{ ref('rs_stg_total_carts') }}