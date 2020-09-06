-- Refresh nightly
{{ config(
	tags='nightly') 
}}

/* 
	Use snapshot functionality to account for previous postcode validity
	Snapshot will introduce SCD2 (Slowly Changing Dimension Type II), which adds:
		dbt_valid_from : what date the row was valid from
		dbt_valid_to : what date the row was valid to (if change occurred), NULL if row is current
		updated_at : what date the particular row was updated
*/	 
{% snapshot users_extract_snapshot %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='updated_at',
        )
    }}

select * from {{ ref('users_extract') }}

{% endsnapshot %}