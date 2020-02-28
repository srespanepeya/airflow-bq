CREATE OR REPLACE TABLE test_datasets.gsod_data_20019_aruy
PARTITION BY date
AS
SELECT s.name , 
       s.country,
       s.state, 
       date(cast(year as int64),cast(mo as int64),cast(da as int64)) as date, 
       (temp - 32) * (5/9) as temp_C,
       fog,
       rain_drizzle,
       hail,
       thunder
FROM `bigquery-public-data.noaa_gsod.gsod2019` a
INNER JOIN `bigquery-public-data.noaa_gsod.stations` s on s.country in ('UY','AR') and s.usaf = a.stn