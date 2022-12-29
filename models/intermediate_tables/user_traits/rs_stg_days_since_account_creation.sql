select 
    {{ var('main_id')}}, 
    least(date(getdate()), {{ get_end_date()}}) - date(min({{ var('col_shopify_identifies_timestamp')}} ))
      as days_since_account_creation
from {{ ref('rs_stg_identifies')}} 
group by 1


