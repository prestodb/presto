SELECT
  "i_item_id"
, "i_item_desc"
, "s_store_id"
, "s_store_name"
, "sum"("ss_net_profit") "store_sales_profit"
, "sum"("sr_net_loss") "store_returns_loss"
, "sum"("cs_net_profit") "catalog_sales_profit"
FROM
  ${database}.${schema}.store_sales
, ${database}.${schema}.store_returns
, ${database}.${schema}.catalog_sales
, ${database}.${schema}.date_dim d1
, ${database}.${schema}.date_dim d2
, ${database}.${schema}.date_dim d3
, ${database}.${schema}.store
, ${database}.${schema}.item
WHERE ("d1"."d_moy" = 4)
   AND ("d1"."d_year" = 2001)
   AND ("d1"."d_date_sk" = "ss_sold_date_sk")
   AND ("i_item_sk" = "ss_item_sk")
   AND ("s_store_sk" = "ss_store_sk")
   AND ("ss_customer_sk" = "sr_customer_sk")
   AND ("ss_item_sk" = "sr_item_sk")
   AND ("ss_ticket_number" = "sr_ticket_number")
   AND ("sr_returned_date_sk" = "d2"."d_date_sk")
   AND ("d2"."d_moy" BETWEEN 4 AND 10)
   AND ("d2"."d_year" = 2001)
   AND ("sr_customer_sk" = "cs_bill_customer_sk")
   AND ("sr_item_sk" = "cs_item_sk")
   AND ("cs_sold_date_sk" = "d3"."d_date_sk")
   AND ("d3"."d_moy" BETWEEN 4 AND 10)
   AND ("d3"."d_year" = 2001)
GROUP BY "i_item_id", "i_item_desc", "s_store_id", "s_store_name"
ORDER BY "i_item_id" ASC, "i_item_desc" ASC, "s_store_id" ASC, "s_store_name" ASC
LIMIT 100
