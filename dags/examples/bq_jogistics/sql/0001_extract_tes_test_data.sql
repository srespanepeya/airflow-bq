INSERT INTO user_matias_menendez.tes_test_2
SELECT 
s.platform, 
s.abTestVariation,
s.userId, 
s.transactionId as order_id,
ord.country_code,
ven.vendor_code as vendor_id,
EXTRACT (DATE FROM DATETIME(ord.order_placed_at, ord.timezone)) as date,
DATETIME(ord.order_placed_at, ord.timezone) as order_placed_at,
EXTRACT (HOUR FROM DATETIME(ord.order_placed_at, ord.timezone)) as order_hour,
CASE
WHEN EXTRACT (HOUR FROM DATETIME(ord.order_placed_at, ord.timezone)) = 12 THEN "Lunch"
WHEN EXTRACT (HOUR FROM DATETIME(ord.order_placed_at, ord.timezone)) = 13 THEN "Lunch"
WHEN EXTRACT (HOUR FROM DATETIME(ord.order_placed_at, ord.timezone)) = 14 THEN "Lunch"
WHEN EXTRACT (HOUR FROM DATETIME(ord.order_placed_at, ord.timezone)) = 20 THEN "Dinner"
WHEN EXTRACT (HOUR FROM DATETIME(ord.order_placed_at, ord.timezone)) = 21 THEN "Dinner"
WHEN EXTRACT (HOUR FROM DATETIME(ord.order_placed_at, ord.timezone)) = 22 THEN "Dinner"
WHEN EXTRACT (HOUR FROM DATETIME(ord.order_placed_at, ord.timezone)) = 23 THEN "Dinner"
ELSE "off peak"
END AS day_bucket,
-- Search promise 
CASE  WHEN s.shopDeliveryTime = "15-30" OR  s.shopDeliveryTime = "15-30 min" THEN "15-30"
      WHEN s.shopDeliveryTime = "30-45" OR  s.shopDeliveryTime = "30-45 min" THEN "30-45"
      WHEN s.shopDeliveryTime = "45-60" OR  s.shopDeliveryTime = "45-60 min" THEN "45-60"
      WHEN s.shopDeliveryTime = "60-90" OR  s.shopDeliveryTime = "60-90 min" THEN "60-90"
      WHEN s.shopDeliveryTime = "90-120" OR  s.shopDeliveryTime = "90-120 min" THEN "90-120"
END AS search_promise, 
-- Actual delivery time in buckets
CASE WHEN ord.timings.actual_delivery_time > 7200 THEN "120 +" 
      WHEN ord.timings.actual_delivery_time > 5400 THEN "90-120" 
      WHEN ord.timings.actual_delivery_time > 3600 THEN "60-90" 
      WHEN ord.timings.actual_delivery_time > 2700 THEN "45-60" 
      WHEN ord.timings.actual_delivery_time > 1800 THEN "30-45" 
      WHEN ord.timings.actual_delivery_time > 900 THEN "15-30" 
      ELSE "0-15"
      END AS actual_DT_buckets,
      
-- Actual DT vs Search Promise       
CASE
WHEN (s.shopDeliveryTime = "15-30" OR  s.shopDeliveryTime = "15-30 min")  AND ord.timings.actual_delivery_time < 900  THEN "1_before"
WHEN (s.shopDeliveryTime = "15-30" OR  s.shopDeliveryTime = "15-30 min")  AND ord.timings.actual_delivery_time < 1800  THEN "2_on time"
WHEN (s.shopDeliveryTime = "15-30" OR  s.shopDeliveryTime = "15-30 min" ) AND ord.timings.actual_delivery_time > 1800  THEN "3_late"
WHEN (s.shopDeliveryTime = "30-45" OR  s.shopDeliveryTime = "30-45 min")  AND ord.timings.actual_delivery_time < 1800  THEN "1_before"
WHEN (s.shopDeliveryTime = "30-45" OR  s.shopDeliveryTime = "30-45 min")  AND ord.timings.actual_delivery_time < 2700  THEN "2_on time"
WHEN (s.shopDeliveryTime = "30-45" OR  s.shopDeliveryTime = "30-45 min")  AND ord.timings.actual_delivery_time > 2700  THEN "3_late"
WHEN (s.shopDeliveryTime = "45-60" OR  s.shopDeliveryTime = "45-60 min" ) AND ord.timings.actual_delivery_time < 2700  THEN "1_before"
WHEN (s.shopDeliveryTime = "45-60" OR  s.shopDeliveryTime = "45-60 min" ) AND ord.timings.actual_delivery_time < 3600  THEN "2_on time"
WHEN (s.shopDeliveryTime = "45-60" OR  s.shopDeliveryTime = "45-60 min" ) AND ord.timings.actual_delivery_time > 3600  THEN "3_late"
WHEN (s.shopDeliveryTime = "60-90" OR  s.shopDeliveryTime = "60-90 min")  AND ord.timings.actual_delivery_time < 3600  THEN "1_before"
WHEN (s.shopDeliveryTime = "60-90" OR  s.shopDeliveryTime = "60-90 min")  AND ord.timings.actual_delivery_time < 5400  THEN "2_on time"
WHEN (s.shopDeliveryTime = "60-90" OR  s.shopDeliveryTime = "60-90 min")  AND ord.timings.actual_delivery_time > 5400  THEN "3_late"
WHEN (s.shopDeliveryTime = "90-120" OR  s.shopDeliveryTime = "90-120 min" ) AND ord.timings.actual_delivery_time < 5400  THEN "1_before"
WHEN (s.shopDeliveryTime = "90-120" OR  s.shopDeliveryTime = "90-120 min" ) AND ord.timings.actual_delivery_time < 7200  THEN "2_on time"
WHEN (s.shopDeliveryTime = "90-120" OR  s.shopDeliveryTime = "90-120 min" ) AND ord.timings.actual_delivery_time > 7200  THEN "3_late"
END as actual_vs_promise, 
ROUND(ord.timings.actual_delivery_time/60,2) as actual_DT_min,
-- Actual DT - MAX Search Promise       
CASE
WHEN (s.shopDeliveryTime = "15-30" OR  s.shopDeliveryTime = "15-30 min")        THEN ROUND(ord.timings.actual_delivery_time/60-30,2)
WHEN (s.shopDeliveryTime = "30-45" OR  s.shopDeliveryTime = "30-45 min")        THEN ROUND(ord.timings.actual_delivery_time/60-45,2)
WHEN (s.shopDeliveryTime = "45-60" OR  s.shopDeliveryTime = "45-60 min")        THEN ROUND(ord.timings.actual_delivery_time/60-60,2)
WHEN (s.shopDeliveryTime = "60-90" OR  s.shopDeliveryTime = "60-90 min")        THEN ROUND(ord.timings.actual_delivery_time/60-90,2)
WHEN (s.shopDeliveryTime = "90-120" OR  s.shopDeliveryTime = "90-120 min" )     THEN ROUND(ord.timings.actual_delivery_time/60-120,2)
END as delay_vs_maxpromise,
-- Search delay over 10
CASE
WHEN (s.shopDeliveryTime = "15-30" OR  s.shopDeliveryTime = "15-30 min")        AND ROUND(ord.timings.actual_delivery_time/60-30,2) > 10 THEN 1
WHEN (s.shopDeliveryTime = "30-45" OR  s.shopDeliveryTime = "30-45 min")        AND ROUND(ord.timings.actual_delivery_time/60-45,2)  > 10 THEN 1 
WHEN (s.shopDeliveryTime = "45-60" OR  s.shopDeliveryTime = "45-60 min")        AND ROUND(ord.timings.actual_delivery_time/60-60,2)  > 10 THEN 1 
WHEN (s.shopDeliveryTime = "60-90" OR  s.shopDeliveryTime = "60-90 min")        AND ROUND(ord.timings.actual_delivery_time/60-90,2)  > 10 THEN 1
WHEN (s.shopDeliveryTime = "90-120" OR  s.shopDeliveryTime = "90-120 min" )     AND ROUND(ord.timings.actual_delivery_time/60-120,2) > 10 THEN 1 
ELSE 0
END as Search_delay_over_10,
-- Order Delay
ROUND(ord.timings.order_delay/60,2) as order_delay,
-- Order Delay > 10 
CASE WHEN ord.timings.order_delay > 600 THEN 1 ELSE 0 END as delay_over_10,
-- Preptime Buckets
CASE
WHEN ord.estimated_prep_time <= 300 THEN "A_0-5"
WHEN ord.estimated_prep_time <= 600 THEN "B_6-10"
WHEN ord.estimated_prep_time <= 900 THEN "C_11-15"
WHEN ord.estimated_prep_time <= 1200 THEN "D_16-20"
WHEN ord.estimated_prep_time <= 1500 THEN "E_21-25"
WHEN ord.estimated_prep_time <= 1800 THEN "F_26-30"
WHEN ord.estimated_prep_time <= 2400 THEN "G_31-40"
ELSE "H_41 +"
END as preptime_bucket,
ROUND(ord.estimated_prep_time/60,2) as preptime,
ord.timings.assumed_actual_preparation_time as actual_preptime,
ord.timings.vendor_late as vendor_late,
ord.timings.at_vendor_time_cleaned as vendor_late_cleaned,
CASE WHEN ord.timings.vendor_late > 600 THEN 1 ELSE 0 END as vendor_late10,
CASE WHEN ord.timings.rider_late > 600 THEN 1 ELSE 0 END as rider_late10,
CASE WHEN os.online_help_clicked IS NULL THEN FALSE ELSE os.online_help_clicked END AS online_help_clicked, 
CASE WHEN os.online_help_clicked IS NULL OR os.online_help_clicked IS FALSE THEN 0 ELSE 1 END AS online_help_clicked_TRUE, 
ROUND(ord.timings.promised_delivery_time/60,0) as promised_DT_hurrier,
ROUND(ord.timings.hold_back_time/60,0) as transmitting_time,
os.online_help_clicked_qty
      
FROM `bpy---pedidosya.Access_Shared_Views.orders_last_shop_delivery_time`  as s
LEFT JOIN `fulfillment-dwh-production.curated_data_shared.orders` as ord on ord.platform_order_code = s.transactionId
LEFT JOIN  `fulfillment-dwh-production.curated_data_shared.vendors` as ven on ord.vendor.id = ven.vendor_id and ord.country_code = ven.country_code
LEFT JOIN `bpy---pedidosya.Access_Shared_Views.Order_Status_Detailed` as os ON  os.platform = s.platform
AND os.fullVisitorId  = s.fullVisitorId 
AND os.visitId  = s.visitId 
AND os.date = s.date
AND os.orderId = s.transactionId 
WHERE DATE(s.date) >=DATE_ADD(CURRENT_DATE,INTERVAL {{ params.from_interval }} DAY)
AND DATE(os.date) >=DATE_ADD(CURRENT_DATE,INTERVAL {{ params.from_interval }} DAY)
AND ord.created_date >=  DATE_ADD(CURRENT_DATE,INTERVAL {{ params.from_interval }} DAY)

AND s.abTestName is not null
AND ord.is_preorder is false
AND ord.order_status = "completed"