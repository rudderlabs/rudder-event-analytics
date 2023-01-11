select device_type, count(*) from {{ ref('rs_stg_all_events') }} where anonymous_id is not null and referrer is not null and context_screen_height is not null
group by 1