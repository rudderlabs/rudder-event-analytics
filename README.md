Using RudderStack eventstream data, this dbt project creates several metrics tables that can be plugged into various visualisation tools such as Tableau or Looker. 

## Setup and Dependencies
* RudderStack event-stream sdk is being used, which creates the following tables in your warehouse:
    * tracks
    * pages
    * identifies
* Run `dbt deps` which installs the required dependencies


### Setting up

All the changes can be done from the `dbt_project.yml` file. The key variables that require to be changed are:

```
vars:
  rs_database: 'rudder_autotrack_data'    #This is the name of database where the RudderStack tables are all stored. Change it to your db name
  rs_schema: 'autotrack'     #This is the name of schema where the RudderStack tables are all stored. Change it to your schema name
  main_id: anonymous_id # Each user is uniquely identified by the anonymous_id generated by RudderStack. This is the column name from the eventstream tables. Change it only if you did any user transformations to modify the name. In majority cases, this should be left untouched.
  start_dt: '2022-09-15' # You may have some old data that's no longer relevant due to either it being incorrect, or it's too old. You can set this to select all events that are generated only after this timestamp. If you do not need such filter, you can set this to some arbitrary very old timestamp (ex: '2000-01-01')

```

## Output:
It creates four tables in your warehouse:
1. rs_stg_sessions_metrics: This table contains all session level information. For each session, what's the session length, the (utm) source of the session, device type etc. 
2. rs_stg_user_first_touch: This table contains all user (anonymous_id) level info. Feach each user, what's their first touch utm source, channel etc
3. rs_metric_dau_based: This table contains the Daily active user counts, split by their first touch acquisition source
4. rs_metric_session_based: This table contains the daily session metrics (count, avg_session_length etc) split by the session source and device dimensions

## Metrics created
1. DAU: Daily Active Users count (a user is counted as DAU if they have atleast one tracks or pages event)
2. total_sessions: Total sessions in a given day. This would be >= DAU, as many users have multiple sessions in a day
3. bounce_rate: If a user visits a single page and their session length is zero, that's counted as a bounced session. This metric shows the percentage of sessions that have zero session length
4. avg_session_length: This is the avg session length in seconds, across all users

