-- database: presto_tpcds; groups: tpcds; requires: com.teradata.tempto.fulfillment.table.hive.tpcds.ImmutableTpcdsTablesRequirements
WITH
  ssr AS (
   SELECT
     "s_store_id" "store_id"
   , "sum"("ss_ext_sales_price") "sales"
   , "sum"(COALESCE("sr_return_amt", 0)) "returns"
   , "sum"(("ss_net_profit" - COALESCE("sr_net_loss", 0))) "profit"
   FROM
     (store_sales
   LEFT JOIN store_returns ON ("ss_item_sk" = "sr_item_sk")
      AND ("ss_ticket_number" = "sr_ticket_number"))
   , date_dim
   , store
   , item
   , promotion
   WHERE ("ss_sold_date_sk" = "d_date_sk")
      AND (CAST("d_date" AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND ("ss_store_sk" = "s_store_sk")
      AND ("ss_item_sk" = "i_item_sk")
      AND ("i_current_price" > 50)
      AND ("ss_promo_sk" = "p_promo_sk")
      AND ("p_channel_tv" = 'N')
   GROUP BY "s_store_id"
) 
, csr AS (
   SELECT
     "cp_catalog_page_id" "catalog_page_id"
   , "sum"("cs_ext_sales_price") "sales"
   , "sum"(COALESCE("cr_return_amount", 0)) "returns"
   , "sum"(("cs_net_profit" - COALESCE("cr_net_loss", 0))) "profit"
   FROM
     (catalog_sales
   LEFT JOIN catalog_returns ON ("cs_item_sk" = "cr_item_sk")
      AND ("cs_order_number" = "cr_order_number"))
   , date_dim
   , catalog_page
   , item
   , promotion
   WHERE ("cs_sold_date_sk" = "d_date_sk")
      AND (CAST("d_date" AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND ("cs_catalog_page_sk" = "cp_catalog_page_sk")
      AND ("cs_item_sk" = "i_item_sk")
      AND ("i_current_price" > 50)
      AND ("cs_promo_sk" = "p_promo_sk")
      AND ("p_channel_tv" = 'N')
   GROUP BY "cp_catalog_page_id"
) 
, wsr AS (
   SELECT
     "web_site_id"
   , "sum"("ws_ext_sales_price") "sales"
   , "sum"(COALESCE("wr_return_amt", 0)) "returns"
   , "sum"(("ws_net_profit" - COALESCE("wr_net_loss", 0))) "profit"
   FROM
     (web_sales
   LEFT JOIN web_returns ON ("ws_item_sk" = "wr_item_sk")
      AND ("ws_order_number" = "wr_order_number"))
   , date_dim
   , web_site
   , item
   , promotion
   WHERE ("ws_sold_date_sk" = "d_date_sk")
      AND (CAST("d_date" AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND ("ws_web_site_sk" = "web_site_sk")
      AND ("ws_item_sk" = "i_item_sk")
      AND ("i_current_price" > 50)
      AND ("ws_promo_sk" = "p_promo_sk")
      AND ("p_channel_tv" = 'N')
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
   , "concat"('store', "store_id") "id"
   , "sales"
   , "returns"
   , "profit"
   FROM
     ssr
UNION ALL    SELECT
     'catalog channel' "channel"
   , "concat"('catalog_page', "catalog_page_id") "id"
   , "sales"
   , "returns"
   , "profit"
   FROM
     csr
UNION ALL    SELECT
     'web channel' "channel"
   , "concat"('web_site', "web_site_id") "id"
   , "sales"
   , "returns"
   , "profit"
   FROM
     wsr
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY "channel" ASC, "id" ASC
LIMIT 100
