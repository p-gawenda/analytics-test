/*
	Incremental option only updates the table with the newest rows based on a specified condition
	Refresh hourly since the pageviews_extract is refreshed hourly
*/
{{ config(
	materialized='incremental',
	tags='hourly') 
}}

select
	user_id,
	url,
	getdate() as view_time -- Add timestamp of the load to indicate at what time a pageview was roughly made
from 'dbt-test'.analytics_test.pageviews_extract

{% if is_incremental() %}

  -- Update rows can have view_time later than the previous latest	
  where view_time > (select max(view_time) from {{ this }})

{% endif %}