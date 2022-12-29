select {{ var('main_id') }}, 
    'first_seen_date' as feature_name, 
     min(date(session_start_time)) as feature_value
from {{ ref('rs_stg_sessions')}}
group by {{ var('main_id') }}