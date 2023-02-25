{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  --where dispatching_base_num is not null 
)
select
    -- identifiers    
    cast(dolocationid as integer) as dropoff_locationid,
    cast(pulocationid as integer) as  pickup_locationid,
    dispatching_base_num,
    -- timestamps
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    cast(pickup_datetime as timestamp) as pickup_datetime,

    SR_Flag,
    Affiliated_base_number    
from tripdata

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
