SELECT
  "count"(DISTINCT "ws_order_number") "order count"
, "round"("sum"("ws_ext_ship_cost"), 2) "total shipping cost"
, "round"("sum"("ws_net_profit"), 2) "total net profit"
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE ("d_date" BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND ("ws1"."ws_ship_date_sk" = "d_date_sk")
   AND ("ws1"."ws_ship_addr_sk" = "ca_address_sk")
   AND ("ca_state" = 'IL')
   AND ("ws1"."ws_web_site_sk" = "web_site_sk")
   AND ("web_company_name" = 'pri')
   AND (EXISTS (
   SELECT *
   FROM
     web_sales ws2
   WHERE ("ws1"."ws_order_number" = "ws2"."ws_order_number")
      AND ("ws1"."ws_warehouse_sk" <> "ws2"."ws_warehouse_sk")
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_returns wr1
   WHERE ("ws1"."ws_order_number" = "wr1"."wr_order_number")
)))
ORDER BY "count"(DISTINCT "ws_order_number") ASC
LIMIT 100
