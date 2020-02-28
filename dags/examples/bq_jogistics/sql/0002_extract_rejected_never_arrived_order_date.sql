INSERT INTO user_matias_menendez.extract_rejected_never_arrived_order_date_2

SELECT 
 r.rider_name
, o.created_date
, r.rider_id
, r.phone_number
, r.email
, o.country_code
, r.batch_number
, cit.name as city_name
, COUNT (DISTINCT o.platform_order_code) as total_orders
, COUNT(DISTINCT CASE WHEN f.reject_message_id = "29" then o.platform_order_code else null end) as never_arrived_orders
, FORMAT_TIMESTAMP("%D", MAX(t.created_at)) as rider_last_order
FROM `bpy---pedidosya.Access_Shared_Views.DWH_fact_orders`  as f
LEFT JOIN `fulfillment-dwh-production.curated_data_shared.orders` as o on f.order_id = o.platform_order_code
LEFT JOIN UNNEST (deliveries) as d
LEFT JOIN UNNEST (transitions) as t
LEFT JOIN `fulfillment-dwh-production.curated_data_shared.riders` as r on r.rider_id=d.rider_id and o.country_code=r.country_code
LEFT JOIN UNNEST (contracts) as cont 
LEFT JOIN `fulfillment-dwh-production.curated_data_shared.countries` as c on o.country_code=c.country_code
LEFT JOIN UNNEST (cities) as cit ON cit.id = cont.city_id

WHERE f.with_logistics is true 
AND d.rider_id is not null
AND t.state = "completed"                 -- rider who compleated the order
AND date(f.partition_date) >= date_add(current_date(), INTERVAL {{ params.from_interval }} DAY)      -- partition name update for optimization and reject reason 29 created at august19
AND o.created_date >= date_add(current_date(),INTERVAL {{ params.from_interval }} DAY)
GROUP BY 1,2,3,4,5,6,7,8