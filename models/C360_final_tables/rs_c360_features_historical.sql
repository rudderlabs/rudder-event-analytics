{{ config( 
    materialized = 'incremental', 
    unique_key = 'row_id',
    incremental_strategy='delete+insert'
   ) 
}} 

{% set numeric_features = dbt_utils.get_column_values(table=ref('rs_c360_long_view'), column='feature_name', where='feature_type=\'numeric\'') %}
{% set string_features = dbt_utils.get_column_values(table=ref('rs_c360_long_view'), column='feature_name', where='feature_type=\'string\'') %}
{% set array_features = dbt_utils.get_column_values(table=ref('rs_c360_long_view'), column='feature_name', where='feature_type=\'array\'') %}

select
    {{ var('main_id')}}, 
    end_timestamp,
    {{concat_columns( [ var('main_id'), date_to_char(get_end_timestamp())])}} as row_id,
    {% for feature_name in numeric_features %}
    max(case when feature_name='{{feature_name}}' then feature_value
                  end) as {{feature_name}},
    {% endfor %} 
     {% for feature_name in string_features%}
    max(case when feature_name='{{feature_name}}' then feature_value  
                  end) as {{feature_name}},
    {% endfor %}    
    {% for feature_name in array_features%}
    max(case when feature_name='{{feature_name}}' then
                {% if target.type == 'redshift' %}
                feature_value
                {% elif target.type == 'snowflake' %}
                array_to_string(feature_value,',') 
                {% endif %}
                end) as {{feature_name}}
                {%- if not loop.last %},{% endif -%}
    {% endfor %}   
from {{ref('rs_c360_long_view')}}
where end_timestamp = {{get_end_timestamp()}}
group by {{ var('main_id')}}, end_timestamp, {{concat_columns( [ var('main_id'), date_to_char(get_end_timestamp())])}}
