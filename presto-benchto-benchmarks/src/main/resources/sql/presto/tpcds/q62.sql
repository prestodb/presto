SELECT
  "substr"("w_warehouse_name", 1, 20)
, "sm_type"
, "web_name"
, "sum"((CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") <= 30) THEN 1 ELSE 0 END)) "30 days"
, "sum"((CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") > 30)
   AND (("ws_ship_date_sk" - "ws_sold_date_sk") <= 60) THEN 1 ELSE 0 END)) "31-60 days"
, "sum"((CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") > 60)
   AND (("ws_ship_date_sk" - "ws_sold_date_sk") <= 90) THEN 1 ELSE 0 END)) "61-90 days"
, "sum"((CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") > 90)
   AND (("ws_ship_date_sk" - "ws_sold_date_sk") <= 120) THEN 1 ELSE 0 END)) "91-120 days"
, "sum"((CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") > 120) THEN 1 ELSE 0 END)) ">120 days"
FROM
  ${database}.${schema}.web_sales
, ${database}.${schema}.warehouse
, ${database}.${schema}.ship_mode
, ${database}.${schema}.web_site
, ${database}.${schema}.date_dim
WHERE ("d_month_seq" BETWEEN 1200 AND (1200 + 11))
   AND ("ws_ship_date_sk" = "d_date_sk")
   AND ("ws_warehouse_sk" = "w_warehouse_sk")
   AND ("ws_ship_mode_sk" = "sm_ship_mode_sk")
   AND ("ws_web_site_sk" = "web_site_sk")
GROUP BY "substr"("w_warehouse_name", 1, 20), "sm_type", "web_name"
ORDER BY "substr"("w_warehouse_name", 1, 20) ASC, "sm_type" ASC, "web_name" ASC
LIMIT 100
