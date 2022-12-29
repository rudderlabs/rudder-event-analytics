 
select {{ var('main_id')}}, 
    'days_since_last_seen' as feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(days_since_last_seen as varchar) as feature_value,
    'numeric' as feature_type 
from {{ref('rs_stg_days_since_last_seen')}}

union all

select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value,
    'string' as feature_type 
from {{ref('rs_stg_first_seen_date')}}

union all 

select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value,
    'string' as feature_type 
from {{ref('rs_stg_last_seen_date')}}

union all 


select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_is_churned_n_days')}} 

union all

select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value,
    'numeric' as feature_type 
from {{ref('rs_stg_active_days_in_past_n_days')}} 


union all
select {{ var('main_id')}}, 
    feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(feature_value as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_session_features')}}