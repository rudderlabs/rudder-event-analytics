{{ config( 
    materialized = 'incremental', 
    unique_key = 'row_id',
    incremental_strategy='delete+insert'
   ) 
}} 

select
   {{ var('main_id')}},
   {{get_end_timestamp()}} as end_timestamp, 
   feature_name,
   feature_value,
   feature_type,
   {{concat_columns( [ var('main_id'), date_to_char(get_end_timestamp()), "feature_name"])}} as row_id 
from
(
    
select {{ var('main_id') }}, feature_name, feature_value, feature_type from {{ ref('rs_stg_grouped_engagement_features') }}

union all 
select {{ var('main_id') }}, feature_name, feature_value, feature_type from {{ ref('rs_stg_grouped_pre_revenue_features') }}

 union all 
select {{ var('main_id') }}, feature_name, feature_value, feature_type from {{ ref('rs_stg_grouped_revenue_features') }}
union all 
select {{ var('main_id') }}, feature_name, feature_value, feature_type from {{ ref('rs_stg_grouped_user_traits') }}
)
