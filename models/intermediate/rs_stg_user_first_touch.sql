select *
from
    (
        select
            {{ var('main_id') }},
            first_value({{ var('col_timestamp') }}) over (
                partition by {{ var('main_id') }}
                order by {{ var('col_timestamp') }} asc
            ) as first_seen_ts,
            first_value(referrer) over (
                partition by {{ var('main_id') }}
                order by {{ var('col_timestamp') }} asc
            ) as referrer,
            first_value(utm_source) over (
                partition by {{ var('main_id') }}
                order by {{ var('col_timestamp') }} asc
            ) as utm_source,
            first_value(utm_medium) over (
                partition by {{ var('main_id') }}
                order by {{ var('col_timestamp') }} asc
            ) as utm_medium,
            first_value(channel) over (
                partition by {{ var('main_id') }}
                order by {{ var('col_timestamp') }} asc
            ) as channel
        from {{ ref('rs_stg_all_events') }}
    )
group by 1, 2, 3, 4, 5, 6
