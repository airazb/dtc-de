-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc-de/data/fhv_tripdata_*']
);

-- Check fhv trip data
SELECT * FROM nytaxi.external_fhv_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE nytaxi.fhv_tripdata_non_partitoned AS
SELECT * FROM nytaxi.external_fhv_tripdata;


CREATE OR REPLACE TABLE `nytaxi.fhv_tripdata_non_partitoned` AS
SELECT * FROM nytaxi.external_fhv_tripdata;


--Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
select count(distinct Affiliated_base_number)
from `nytaxi.external_fhv_tripdata` ;

select count(distinct Affiliated_base_number)
from `nytaxi.fhv_tripdata_non_partitoned`;

--How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
select count(1)
from `nytaxi.fhv_tripdata_non_partitoned`
where PUlocationID is null and DOlocationID is null;
--result 717748


/*What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

Cluster on pickup_datetime Cluster on affiliated_base_number
Partition by pickup_datetime Cluster on affiliated_base_number - OK
Partition by pickup_datetime Partition by affiliated_base_number
Partition by affiliated_base_number Cluster on pickup_datetime
*/

/*Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.*/
-- Creating a partition and cluster table

CREATE OR REPLACE TABLE nytaxi.fhv_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM nytaxi.external_fhv_tripdata;

SELECT distinct affiliated_base_number as dist_affiliated_base_number
FROM nytaxi.fhv_tripdata_partitoned_clustered
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT distinct affiliated_base_number as dist_affiliated_base_number
FROM nytaxi.fhv_tripdata_non_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

--647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

/*Where is the data stored in the External Table you created?

Big Query
GCP Bucket - OK
Container Registry
Big Table*/

