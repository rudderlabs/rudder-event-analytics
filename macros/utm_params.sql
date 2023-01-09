
{% macro get_utm_params(table_name) %}
with
    cte_referrer as (
        select
            {{ var("main_id") }},
            {{ var("col_timestamp") }},
            {{ var("col_session_id") }},
            case
                when lower({{ var('col_referrer') }}) = '$direct'
                then 'direct'
                else
                    regexp_replace(
                        regexp_replace({{ var('col_referring_domain') }}, '^www\.', ''),
                        '\.com(\.\w+)?$|\.co(\.\w+)?$|\.org$',
                        ''
                    )
            end as referrer,
            {{ var('col_utm_source') }},
            {{ var('col_utm_medium') }}
        from {{ table_name }}
    )
        select
            {{ var("main_id") }},
            {{ var("col_timestamp") }},
            {{ var("col_session_id") }},
            referrer,
            case
                when
                    lower(coalesce({{ var('col_utm_source') }}, 'null')) != 'null'
                    and {{ var('col_utm_source') }} != ''
                then lower({{ var('col_utm_source') }})
                else referrer
            end as utm_source,
            case
                when
                    lower(coalesce({{ var('col_utm_medium') }}, 'null')) != 'null'
                    and {{ var('col_utm_medium') }} != ''
                then lower({{ var('col_utm_medium') }})
                else referrer
            end as utm_medium
        from cte_referrer
    {% endmacro %}

{% macro get_channel(utm_source, utm_medium, referrer) %}
case 
	when {{utm_source}} = 'direct' or coalesce({{utm_medium}}, 'none') = 'none' then 'Direct'
	when {{utm_medium}} = 'organic' then 'Organic Search'
  when {{utm_medium}} in ('social', 'social-network','social-media','sm','social network','social media') then 'Social'
  when {{utm_medium}} = 'email' then 'Email'
  when {{utm_medium}} = 'affiliate' then 'Affiliates'
  when {{utm_medium}} = 'referral' then 'Referral'
  when {{utm_medium}} in ('cpc','ppc','paidsearch') then 'Paid Search'
  when {{utm_medium}} in ('cpv', 'cpa', 'cpp', 'content-text') then 'Other Advertising'
  when {{utm_medium}} in ('display', 'cpm', 'banner') then 'Display'
  else '(other)' end
{% endmacro %}

/*
ToDo:
Review above definitions
Above channel mapping is from Google, while the source/medium mapping is from internal. Does this look fine? Some potential misses:
1. Not picking Organic Search as  we are not sending medium as organic.
2. Not using campaign name in our filters
3. Our channels don't align with that of Google. Ex: Google doesn't have Paid Display or Paid Social while we do. We don't have Social while Google does. 
*/