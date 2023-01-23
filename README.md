# DataTalksClud-Data engineering zoomcamp (cohort 2023) 

Learning (week 1):
* Docker and SQL
  * [Install and setup](#install-setup)
  * [Run docker](#run-docker)
  * [Create pipeline](#create-pipeline)
  * [SQL for analysis data](#sql)

## Install & setup
- Install docker
- Setup wsl
- Create venv for python project
  
## Run docker
[Create docker container and run](dockerfile)

## Create pipeline
[Create EL pipeline for load data to pgsql](ingest_data.py)

## SQL 
Run query in pgcli
```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```
Get count taxi trip for 2019-01-21

```sql
select 
    count(1) 
from green_taxi_trips 
where 
    lpep_dropoff_datetime < TO_DATE('2019-01-22','yyyy-MM-dd') 
and lpep_pickup_datetime >= TO_DATE('2019-01-21', 'yyyy-MM-dd')   
```


