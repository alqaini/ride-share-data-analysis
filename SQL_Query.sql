CREATE OR REPLACE TABLE `my-project-11-394504.uber_data_analysis.final_table` AS (
SELECT

fact.trip_id,
fact.VendorID,
dat.tpep_pickup_datetime,
dat.tpep_dropoff_datetime,
dist.trip_distance,
rate.rate_code_name,
pickup.pickup_latitude,
pickup.pickup_longitude,
dropoff.dropoff_latitude,
dropoff.dropoff_longitude,
pay.payment_type_name,
pay.payment_type,
fact.fare_amount,
fact.extra,
fact.tip_amount,
fact.improvement_surcharge,
fact.mta_tax

FROM `my-project-11-394504.uber_data_analysis.fact_table` fact
JOIN `my-project-11-394504.uber_data_analysis.distance_table` dist ON fact.trip_distance_id = dist.trip_distance_id
JOIN `my-project-11-394504.uber_data_analysis.datetime_table` dat ON dat.datetime_id = fact.datetime_id
JOIN `my-project-11-394504.uber_data_analysis.dropoff_location_table` dropoff ON dropoff.dropoff_id = fact.dropoff_id
JOIN `my-project-11-394504.uber_data_analysis.passenger_count_table` pcount ON pcount.passenger_count_id = fact.passenger_count_id  
JOIN `my-project-11-394504.uber_data_analysis.payment_table` pay ON pay.payment_type_id = fact.payment_type_id
JOIN `my-project-11-394504.uber_data_analysis.pickup_location_table` pickup ON pickup.pickup_id = fact.pickup_id
JOIN `my-project-11-394504.uber_data_analysis.rate_code_table` rate ON rate.rate_code_id = fact.rate_code_id);