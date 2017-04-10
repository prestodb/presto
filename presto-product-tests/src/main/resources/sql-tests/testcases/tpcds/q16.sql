-- database: presto_tpcds; groups: tpcds; requires: com.teradata.tempto.fulfillment.table.hive.tpcds.ImmutableTpcdsTablesRequirements
SELECT
  "count"(DISTINCT "cs_order_number") "order count"
, "sum"("cs_ext_ship_cost") "total shipping cost"
, "sum"("cs_net_profit") "total net profit"
FROM
  catalog_sales cs1
, date_dim
, customer_address
, call_center
WHERE ("d_date" BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND ("cs1"."cs_ship_date_sk" = "d_date_sk")
   AND ("cs1"."cs_ship_addr_sk" = "ca_address_sk")
   AND ("ca_state" = 'GA')
   AND ("cs1"."cs_call_center_sk" = "cc_call_center_sk")
   AND ("cc_county" IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
   AND (EXISTS (
   SELECT *
   FROM
     catalog_sales cs2
   WHERE ("cs1"."cs_order_number" = "cs2"."cs_order_number")
      AND ("cs1"."cs_warehouse_sk" <> "cs2"."cs_warehouse_sk")
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     catalog_returns cr1
   WHERE ("cs1"."cs_order_number" = "cr1"."cr_order_number")
)))
ORDER BY "count"(DISTINCT "cs_order_number") ASC
LIMIT 100
