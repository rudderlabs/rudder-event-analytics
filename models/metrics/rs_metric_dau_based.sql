
select
    event_date,
    referrer,
    utm_source,
    utm_medium,
    channel,
    count(distinct b.{{var('main_id') }}) as dau
from
    (
        select {{ var('main_id') }}, referrer, utm_source, utm_medium, channel
        from {{ ref('rs_stg_user_first_touch') }}
    ) a
left join
    (
        select distinct event_date, {{ var('main_id') }}
        from {{ ref('rs_stg_session_metrics') }} 
    ) b
    on a.{{ var('main_id') }} = b.{{ var('main_id') }}
group by event_date, referrer, utm_source, utm_medium, channel
order by event_date desc
