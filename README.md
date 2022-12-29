Using RudderStack eventstream data, this dbt project creates several metrics tables that can be plugged into various visualisation tools such as Tableau or Looker. 

## Setup and Dependencies
* RudderStack event-stream sdk is being used, which creates the following tables in your warehouse:
    * tracks
    * pages
    * identifies
* Run `dbt deps` which installs the required dependencies

### Setting up ID Stitch configuration
* Set up a dbt profile with the right warehouse credentials
* Go to dbt_packages/id_stitching/dbt_project.yml and modify the variables `schemas-to-include`, and `source-database` to the schema and database where the rudderstack eventstream tables are present. The source-database is case sensitive. Ensure it is maintained, while the schemas is entered in lowercase. Also set  `tables-to-include` to ('pages', 'tracks', 'identifies') so that the id stitcher runs on only these tables


2. [RudderStack ID stitch dbt](https://hub.getdbt.com/rudderlabs/id_stitching/latest/) is enabled and an id graph table is generated in the warehouse

### Setting up

All the changes can be done from the `dbt_project.yml` file. The key variables that require to be changed are:

```

vars:
  shopify_database: 'warehouse'    #This is the name of database where the RudderStack Shopify tables are all stored
  shopify_schema: 'rudderstack'     #This is the name of schema where the RudderStack Shopify tables are all stored
  start_date: '2015-01-01'              #This is the lower bound on date. Only events after this date would be considered. Typically used to ignore data during test setup days. 
  end_date: 'now'                       #This is the upper bound on date .Default is 'now'; It can be modified to some old date to create snapshot of features as of an older timestamp
  date_format: 'YYYY-MM-DD'             # This is the date format
  session_cutoff_in_sec: 1800           # A session is a continuous sequence of events, as long as the events are within this interval. If two consecutive events occur at an interval of greater than this, a new session is created.
  lookback_days: [1,7,30,90,365]          # There are various lookback features such as amt_spent_in_past_n_days etc where n takes values from this list. If the list has two values [7, 30], amt_spent_in_past_7_days and amt_spent_in_past_30_days are computed for ex.
  product_ref_var: 'product_id'         #This is the name of the property in the tracks calls that uniquely corresponds to the product
  category_ref_var: 'category_l1'       #This is the name of the property in the tracks calls that corresponds to the product category
  main_id: 'rudder_id' # Column name of the main id/rudder id from id graph table
  card_types: ('mastercard', 'visa')    #These are the types of credit cards(in lowercase) that will be considered to check if the user has a credit card 
 

```

Along with the above variables, the table names (variables that start with `tbl_` prefix) may need to be changed depending on the schema, both in the dbt_project.yml file and in schema.yml file if they deviate from the shopify spec. 



### Point-in-time correctness
The `end_date` variable in `dbt_project.yml` can be used to generate the features as of some time in the past. The default value is `'now'` which generates the features as of the run timestamp. But if it is modified to some date in the past, only those events till that timestamp are considered to generate the features. This is a valuable functionality while generating training data for various Machine Learning models, which require historical data to train predictive models.


## Configuration

### Customisation

## Output:
[TODO]

## Metrics created


### Appendix (Delete later)
### Features List

- [traits features](https://github.com/rudderlabs/dbt_shopify_features/tree/main/models/intermediate_tables/rs_user_traits): These are related to user, which do not, or rarely change. They are created from the latest identify call for a given user
    1. gender (char)
    2. age_in_years (int)
    3. country (str)
    4. state (str)
    5. email_domain (str)
    6. has_mobile_app (bool)
    7. campaign_sources (Array[str])
    8. is_active_on_website (bool)
    9. days_since_account_creation (int)
    10. payment_modes <list of all payment modes ever (cod, credit card, debit card, wallet etc)> (List[str])
    11. customer_currency (str) 
    12. card_network (str)
    13. first_name (str)
    14. last_name (str)
    15. device_manufacturer (can take the latest)
    16. device_name (str)
    17. device_type (str)
    18. email (str)
- [Engagement features](https://github.com/rudderlabs/dbt_shopify_features/tree/main/models/intermediate_tables/rs_engagement_features): These are related to the time a user spends on the website/app.
    1. days_since_last_seen (int)
    2. is_churned_n_days (bool)
    3. active_days_in_past_n_days (int)
    4. Session based:  (using session_id from the sdk)
        1. avg_session_length_n_days (float)
        2. total_sessions_till_date (int)  
        3. avg_session_length_till_date (float)
        4. total_sessions_n_days (int)
    5. last_seen_date (str)
    6. first_seen_date (str)
- [Revenue based features](https://github.com/rudderlabs/dbt_shopify_features/tree/main/models/intermediate_tables/rs_revenue_features): These are related to the amount users spend
    1. net_amt_spent_in_past_n_days (float)
    2. net_amt_spent_in_past (float); (sales - refund)
    3. last_transaction_value (float)
    4. average_transaction_value (float)
    5. average_units_per_transaction (float)
    6. median_transaction_value (float)
    7. highest_transaction_value (float)
    8. days_since_last_purchase (int)
    9. transactions_in_past_n_days (int)
    10. total_transactions (int)
    11. has_credit_card (bool)
    12. days_since_first_purchase (int)
    13. gross_amt_spent_in_past (float); (overall and in past n days)
    14. refund (overall and in past n days)
    15. refund_count (int)
- [Pre-revenue features](https://github.com/rudderlabs/dbt_shopify_features/tree/main/models/intermediate_tables/rs_pre_revenue_features): Related to the pre-checkout engagement features such as cart adds, abandoned carts etc
    1. carts_in_past_n_days
    2. total_carts
    3. [TODO]products_added_in_past_n_days (both purchased and not purchased)
    4. [TODO]total_products_added_to_cart
    5. days_since_last_cart_add (int)
    6. [TODO]last_cart_status 
    7. [TODO]dollar_value_of_items_in_cart
    8. [TODO]items_in_cart
- [SKU based features](https://github.com/rudderlabs/dbt_shopify_features/tree/main/models/intermediate_tables/rs_sku_features): Related to the SKU, categories etc. 
    1. [TODO]items_purchased_ever (List of unique product items, List[str]); Max size is controlled from variable `var_max_list_size`; IF a user has more than this number, such users get null result. If this number is too large, it can create performance issues.
    2. [TODO]categories_purchased_ever (List of unique category ids, List[str]); Max size is controlled from variable `var_max_list_size`; IF a user has more than this number, such users get null result. If this number is too large, it can create performance issues.
    3. [TODO]highest_spent_category (str) (based on the price of the products); Max size is controlled from variable `var_max_cart_size`; If a user has more than this number of distinct categories, it limits to the random `var_max_cart_size` number of categories. If this number is too large, it can create performance issues.
    4. [TODO]highest_transacted_category (str); Max size is controlled from variable `var_max_cart_size`; If a user has more than this number of distinct categories, it limits to the random `var_max_cart_size` number of categories. If this number is too large, it can create performance issues.


# Questions:
1. Do we assume session_id to be present? 
   1.1 Always create
   1.2 Always use (if column is present)
   1.3 Create if value is null, else use - more complex and edge cases
2. How do we integrate the id graph? Has one approach currently. Is that good enough?
3. Incremental mode vs full batch mode - full batch is easy, but costly on compute. v0 can be full batch and v1 can be incremental? Extra effort but faster release.
4. Granularity - daily is sufficient? 