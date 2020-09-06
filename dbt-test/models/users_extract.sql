-- Refresh nightly
{{ config(
	tags='nightly') 
}}

select
	id,
	postcode,
	cast(getdate() as date) as updated_at -- Add date of the load to the query
from 'dbt-test'.analytics_test.users_extract