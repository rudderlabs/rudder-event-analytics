with
    cte_session_info as (
        select
            *, datediff(second, session_start_time, session_end_time) as session_length
        from
            (
                select
                    {{ var('main_id') }},
                    {{ to_date(var('col_timestamp')) }} as event_date,
                    first_value(referrer) over (
                        partition by {{ var('main_id') }}, {{ var('col_session_id') }}
                        order by {{ var('col_timestamp') }} asc rows between unbounded preceding and unbounded following
                    ) as referrer,
                    first_value(source_medium) over (
                        partition by {{ var('main_id') }}, {{ var('col_session_id') }}
                        order by {{ var('col_timestamp') }} asc rows between unbounded preceding and unbounded following
                    ) as source_medium,
                    first_value(channel) over (
                        partition by {{ var('main_id') }}, {{ var('col_session_id') }}
                        order by {{ var('col_timestamp') }} asc rows between unbounded preceding and unbounded following
                    ) as channel,
                    first_value(device_type) over (
                        partition by {{ var('main_id') }}, {{ var('col_session_id') }}
                        order by {{ var('col_timestamp') }} asc rows between unbounded preceding and unbounded following
                    ) as device_type,
                    {{ var('col_session_id') }},
                    first_value({{ var('col_timestamp') }}) over (
                        partition by {{ var('main_id') }}, {{ var('col_session_id') }}
                        order by {{ var('col_timestamp') }} asc rows between unbounded preceding and unbounded following
                    ) as session_start_time,
                    first_value({{ var('col_timestamp') }}) over (
                        partition by {{ var('main_id') }}, {{ var('col_session_id') }}
                        order by {{ var('col_timestamp') }} desc rows between unbounded preceding and unbounded following
                    ) as session_end_time
                from {{ ref('rs_stg_all_events') }} where {{ var('col_session_id') }} is not null
            ) a 
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
select
    {{ var('main_id') }},
    event_date,
    referrer,
    source_medium,
    channel,
    device_type,
    count(distinct {{ var('col_session_id') }}) as n_sessions,
    sum(session_length) as total_session_length,
    sum(case when session_length = 0 then 1 else 0 end) as bounced_sessions
from cte_session_info
group by {{ var('main_id') }}, event_date, referrer, source_medium, channel,device_type
