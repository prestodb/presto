SELECT
  "w_state"
, "i_item_id"
, "sum"((CASE WHEN (CAST("d_date" AS DATE) < CAST('2000-03-11' AS DATE)) THEN ("cs_sales_price" - COALESCE("cr_refunded_cash", 0)) ELSE 0 END)) "sales_before"
, "sum"((CASE WHEN (CAST("d_date" AS DATE) >= CAST('2000-03-11' AS DATE)) THEN ("cs_sales_price" - COALESCE("cr_refunded_cash", 0)) ELSE 0 END)) "sales_after"
FROM
  (${database}.${schema}.catalog_sales
LEFT JOIN ${database}.${schema}.catalog_returns ON ("cs_order_number" = "cr_order_number")
   AND ("cs_item_sk" = "cr_item_sk"))
, ${database}.${schema}.warehouse
, ${database}.${schema}.item
, ${database}.${schema}.date_dim
WHERE ("i_current_price" BETWEEN DECIMAL '0.99' AND DECIMAL '1.49')
   AND ("i_item_sk" = "cs_item_sk")
   AND ("cs_warehouse_sk" = "w_warehouse_sk")
   AND ("cs_sold_date_sk" = "d_date_sk")
   AND (CAST("d_date" AS DATE) BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
GROUP BY "w_state", "i_item_id"
ORDER BY "w_state" ASC, "i_item_id" ASC
LIMIT 100
