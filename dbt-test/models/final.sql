-- Refresh hourly
{{ config(
	tags='hourly') 
}}

-- Instantiate full historic view of users_extract
with users as (

	select * from {{ ref('users_extract_snapshot') }}

),

-- Instatiate pageviews_extract
pageviews as (
	
	select * from {{ ref('pageviews_extract') }}

),

-- Model table used for current postcode analysis
current_postcode as (
	
	select 
		users.postcode as CurrentPostcode,
		-- Since we are only interested in hours and lesser granularity: trim minutes, seconds and smaller to remove any discrepancies during load time
		dateadd(hour, datediff(hour, 0, pageviews.view_time), 0) as DateHour,
		count(pageviews.user_id) as [PageViews]
	from users
	join pageviews on pageviews.user_id = users.id and pageviews.view_time >= users.dbt_valid_from -- join on user and pageviews that happened in current postcode only
	where users.dbt_valid_to is null -- Filter only on current postcode
	group by 1, 2

),

-- Model table used for previous postcode analysis
previous_postcode as (

	select
		users.postcode as PreviousPostcode,
		-- Same as above
		dateadd(hour, datediff(hour, 0, pageviews.view_time), 0) as DateHour,
		count(pageviews.user_id) as [PageViews]
	from users
	join pageviews on pageviews.user_id = users.id and pageviews.view_time between users.dbt_valid_from and users.dbt_valid_to -- join on user and pageviews that happened when a particular postocde was valid
	where users.dbt_valid_to is not null -- Filter on non-current postcodes
	group by 1, 2

),

-- Join current and previous postcodes together to get total views per postcode per time period
final as (

	select
		coalesce(current_postcode.CurrentPostcode, previous_postcode.PreviousPostcode) as Postcode,
		coalesce(current_postcode.DateHour, previous_postcode.DateHour) as DateHour,
		coalesce(current_postcode.PageViews, 0) + coalesce(previous_postcode.PageViews, 0) as TotalPageViews
	from current_postcode
	full join previous_postcode on current_postcode.CurrentPostcode = previous_postcode.PreviousPostcode
								and current_postcode.DateHour = previous_postcode.DateHour

)

select * from final	