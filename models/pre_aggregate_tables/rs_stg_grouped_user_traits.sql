{{ config( 
    materialized = 'table'
   ) 
}} 
select {{ var('main_id')}}, 
    'email' as feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(email as varchar) as feature_value, 
    'string' as feature_type 
from {{ref('rs_stg_email')}}

union all

select {{ var('main_id')}}, 
    'email_domain' as feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(email_domain as varchar) as feature_value, 
    'string' as feature_type 
from {{ref('rs_stg_email_domain')}}

union all

select {{ var('main_id')}}, 
    'days_since_account_creation' as feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(days_since_account_creation as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_days_since_account_creation')}}

union all

select {{ var('main_id')}}, 
    'has_mobile_app' as feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(has_mobile_app as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_has_mobile_app')}}

union all

select {{ var('main_id')}}, 
    'is_active_on_website' as feature_name, 
    {{get_end_timestamp()}} as end_timestamp, 
    cast(is_active_on_website as varchar) as feature_value, 
    'numeric' as feature_type 
from {{ref('rs_stg_is_active_on_website')}}

{% if var('col_shopify_identifies_birthday') != '' %}
    union all
    select {{ var('main_id')}}, 
        'age' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(age as varchar) as feature_value, 
        'numeric' as feature_type 
    from {{ref('rs_stg_age')}}
{% endif %}

{% if var('col_shopify_identifies_state') != '' %}
    union all
    select {{ var('main_id')}}, 
        'state' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(state as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_state')}}
{% endif %}

{% if var('col_shopify_identifies_country') != '' %}
    union all
    select {{ var('main_id')}}, 
        'country' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(country as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_country')}}
{% endif %}

{% if var('col_shopify_identifies_first_name') != '' %}
    union all
    select {{ var('main_id')}}, 
        'first_name' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(first_name as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_first_name')}}
{% endif %}

{% if var('col_shopify_identifies_last_name') != '' %}
    union all
    select {{ var('main_id')}}, 
        'last_name' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(last_name as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_last_name')}}
{% endif %}

{% if var('col_shopify_identifies_gender') != '' %}
    union all
    select {{ var('main_id')}}, 
        'gender' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(gender as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_gender')}}
{% endif %}

{% if var('col_shopify_identifies_currency') != '' %}
    union all
    select {{ var('main_id')}}, 
        'customer_currency' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(customer_currency as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_customer_currency')}}
{% endif %}

{% if var('col_shopify_identifies_payment_method') != '' %}
    union all
    select {{ var('main_id')}}, 
        'payment_modes' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(payment_modes as varchar) as feature_value, 
        'array' as feature_type 
    from {{ref('rs_stg_payment_modes')}}
{% endif %}

{% if var('col_shopify_identifies_device_name') != '' %}
    union all
    select {{ var('main_id')}}, 
        'device_name' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(device_name as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_device_name')}}
{% endif %}

{% if var('col_shopify_identifies_device_type') != '' %}
    union all
    select {{ var('main_id')}}, 
        'device_type' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(device_type as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_device_type')}}
{% endif %}

{% if var('col_shopify_identifies_device_manufacturer') != '' %}
    union all
    select {{ var('main_id')}}, 
        'device_manufacturer' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(device_manufacturer as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_device_manufacturer')}}
{% endif %}

{% if var('col_shopify_identifies_card_network') != '' %}
    union all
    select {{ var('main_id')}}, 
        'card_network' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(card_network as varchar) as feature_value, 
        'string' as feature_type 
    from {{ref('rs_stg_card_network')}}
{% endif %}

{% if var('col_shopify_identifies_campaign_source') != '' %}
    union all
    select {{ var('main_id')}}, 
        'campaign_sources' as feature_name, 
        {{get_end_timestamp()}} as end_timestamp, 
        cast(campaign_sources as varchar) as feature_value, 
        'array' as feature_type 
    from {{ref('rs_stg_campaign_sources')}}
{% endif %}