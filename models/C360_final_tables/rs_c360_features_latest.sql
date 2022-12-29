{{ config( 
    materialized = 'view'
   ) 
}} 

select * from {{ ref('rs_c360_features_historical') }}
where end_timestamp = (select max(end_timestamp) from {{ ref('rs_c360_features_historical') }} )