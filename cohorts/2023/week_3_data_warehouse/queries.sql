CREATE OR REPLACE TABLE `dezoomcamp_2.fhv_tripdata_partitioned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS 
SELECT * FROM `data-engineer-zoomcamp-376119.dezoomcamp_2.fhv_tripdata`

SELECT DISTINCT COUNT(affiliated_base_number) 
FROM `data-engineer-zoomcamp-376119.dezoomcamp_2.fhv_tripdata_partitioned` 
WHERE 
 DATE(pickup_datetime) >= cast("2019-03-01" as date)  AND
 DATE(pickup_datetime) <= cast("2019-03-31" as date);