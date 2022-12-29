select {{ var('main_id') }}, 
    'last_seen_date' as feature_name, 
     max(date(session_end_time)) as feature_value
from {{ ref('rs_stg_sessions')}}
group by {{ var('main_id') }}