-- database: presto_tpcds; groups: tpcds; requires: com.teradata.tempto.fulfillment.table.hive.tpcds.ImmutableTpcdsTablesRequirements
WITH
  ssr AS (
   SELECT
     "s_store_id"
   , "sum"("sales_price") "sales"
   , "sum"("profit") "profit"
   , "sum"("return_amt") "returns"
   , "sum"("net_loss") "profit_loss"
   FROM
     (
      SELECT
        "ss_store_sk" "store_sk"
      , "ss_sold_date_sk" "date_sk"
      , "ss_ext_sales_price" "sales_price"
      , "ss_net_profit" "profit"
      , CAST(0 AS DECIMAL(7,2)) "return_amt"
      , CAST(0 AS DECIMAL(7,2)) "net_loss"
      FROM
        store_sales
UNION ALL       SELECT
        "sr_store_sk" "store_sk"
      , "sr_returned_date_sk" "date_sk"
      , CAST(0 AS DECIMAL(7,2)) "sales_price"
      , CAST(0 AS DECIMAL(7,2)) "profit"
      , "sr_return_amt" "return_amt"
      , "sr_net_loss" "net_loss"
      FROM
        store_returns
   )  salesreturns
   , date_dim
   , store
   WHERE ("date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND ("store_sk" = "s_store_sk")
   GROUP BY "s_store_id"
) 
, csr AS (
   SELECT
     "cp_catalog_page_id"
   , "sum"("sales_price") "sales"
   , "sum"("profit") "profit"
   , "sum"("return_amt") "returns"
   , "sum"("net_loss") "profit_loss"
   FROM
     (
      SELECT
        "cs_catalog_page_sk" "page_sk"
      , "cs_sold_date_sk" "date_sk"
      , "cs_ext_sales_price" "sales_price"
      , "cs_net_profit" "profit"
      , CAST(0 AS DECIMAL(7,2)) "return_amt"
      , CAST(0 AS DECIMAL(7,2)) "net_loss"
      FROM
        catalog_sales
UNION ALL       SELECT
        "cr_catalog_page_sk" "page_sk"
      , "cr_returned_date_sk" "date_sk"
      , CAST(0 AS DECIMAL(7,2)) "sales_price"
      , CAST(0 AS DECIMAL(7,2)) "profit"
      , "cr_return_amount" "return_amt"
      , "cr_net_loss" "net_loss"
      FROM
        catalog_returns
   )  salesreturns
   , date_dim
   , catalog_page
   WHERE ("date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND ("page_sk" = "cp_catalog_page_sk")
   GROUP BY "cp_catalog_page_id"
) 
, wsr AS (
   SELECT
     "web_site_id"
   , "sum"("sales_price") "sales"
   , "sum"("profit") "profit"
   , "sum"("return_amt") "returns"
   , "sum"("net_loss") "profit_loss"
   FROM
     (
      SELECT
        "ws_web_site_sk" "wsr_web_site_sk"
      , "ws_sold_date_sk" "date_sk"
      , "ws_ext_sales_price" "sales_price"
      , "ws_net_profit" "profit"
      , CAST(0 AS DECIMAL(7,2)) "return_amt"
      , CAST(0 AS DECIMAL(7,2)) "net_loss"
      FROM
        web_sales
UNION ALL       SELECT
        "ws_web_site_sk" "wsr_web_site_sk"
      , "wr_returned_date_sk" "date_sk"
      , CAST(0 AS DECIMAL(7,2)) "sales_price"
      , CAST(0 AS DECIMAL(7,2)) "profit"
      , "wr_return_amt" "return_amt"
      , "wr_net_loss" "net_loss"
      FROM
        (web_returns
      LEFT JOIN web_sales ON ("wr_item_sk" = "ws_item_sk")
         AND ("wr_order_number" = "ws_order_number"))
   )  salesreturns
   , date_dim
   , web_site
   WHERE ("date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND ("wsr_web_site_sk" = "web_site_sk")
   GROUP BY "web_site_id"
) 
SELECT
  "channel"
, "id"
, "sum"("sales") "sales"
, "sum"("returns") "returns"
, "sum"("profit") "profit"
FROM
  (
   SELECT
     'store channel' "channel"
   , "concat"('store', "s_store_id") "id"
   , "sales"
   , "returns"
   , ("profit" - "profit_loss") "profit"
   FROM
     ssr
UNION ALL    SELECT
     'catalog channel' "channel"
   , "concat"('catalog_page', "cp_catalog_page_id") "id"
   , "sales"
   , "returns"
   , ("profit" - "profit_loss") "profit"
   FROM
     csr
UNION ALL    SELECT
     'web channel' "channel"
   , "concat"('web_site', "web_site_id") "id"
   , "sales"
   , "returns"
   , ("profit" - "profit_loss") "profit"
   FROM
     wsr
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY "channel" ASC, "id" ASC
LIMIT 100
