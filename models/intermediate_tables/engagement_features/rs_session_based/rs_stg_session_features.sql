with cte_sessions as (
    select * from {{ref('rs_stg_sessions')}}
)

select {{ var('main_id') }}, 
    'total_sessions_till_date' as feature_name, 
    max(session_id)+1 as feature_value 
from cte_sessions 
group by {{ var('main_id') }}
union all
select {{ var('main_id') }}, 
    'total_sessions_last_week' as feature_name, 
    max(session_id)+1 as feature_value 
from cte_sessions 
where datediff(day, date(session_start_time), date({{get_end_timestamp()}})) between 0 and 7 
group by {{ var('main_id') }}
union all
select {{ var('main_id') }}, 
    'avg_session_length_overall' as feature_name, 
    avg(datediff(second, session_start_time, session_end_time)::real) as feature_value 
from cte_sessions 
group by {{ var('main_id') }}
union all
select {{ var('main_id') }}, 
    'avg_session_length_last_week' as feature_name, (avg(datediff(second, session_start_time, session_end_time)::real)) as feature_value
from cte_sessions
where datediff(day, date(session_start_time), date({{get_end_timestamp()}})) between 0 and 7 
group by {{ var('main_id') }}