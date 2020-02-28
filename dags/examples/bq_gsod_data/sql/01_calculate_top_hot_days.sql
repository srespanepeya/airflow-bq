CREATE OR REPLACE TABLE user_matias_menendez.gsod_top_hot_days
AS
SELECT name, 
       country,
       state, 
       ARRAY_AGG(STRUCT(date,temp_c) ORDER BY temp_c DESC LIMIT 5) as top_hot, 
       MIN(date) as active_from,
       MAX(date) as active_until
FROM `bpy---pedidosya.test_datasets.gsod_data_20019_aruy`  

WHERE date >= '2019-01-01'
GROUP BY 1,2,3
ORDER BY 1 ASC, 2 ASC, 3 DESC