WITH
  ss AS (
   SELECT
     "s_store_sk"
   , "round"("sum"("ss_ext_sales_price"), 2) "sales"
   , "round"("sum"("ss_net_profit"), 2) "profit"
   FROM
     store_sales
   , date_dim
   , store
   WHERE ("ss_sold_date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND ("ss_store_sk" = "s_store_sk")
   GROUP BY "s_store_sk"
) 
, sr AS (
   SELECT
     "s_store_sk"
   , "round"("sum"("sr_return_amt"), 2) "returns"
   , "round"("sum"("sr_net_loss"), 2) "profit_loss"
   FROM
     store_returns
   , date_dim
   , store
   WHERE ("sr_returned_date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND ("sr_store_sk" = "s_store_sk")
   GROUP BY "s_store_sk"
) 
, cs AS (
   SELECT
     "cs_call_center_sk"
   , "round"("sum"("cs_ext_sales_price"), 2) "sales"
   , "round"("sum"("cs_net_profit"), 2) "profit"
   FROM
     catalog_sales
   , date_dim
   WHERE ("cs_sold_date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY "cs_call_center_sk"
) 
, cr AS (
   SELECT
     "cr_call_center_sk"
   , "round"("sum"("cr_return_amount"), 2) "returns"
   , "round"("sum"("cr_net_loss"), 2) "profit_loss"
   FROM
     catalog_returns
   , date_dim
   WHERE ("cr_returned_date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY "cr_call_center_sk"
) 
, ws AS (
   SELECT
     "wp_web_page_sk"
   , "round"("sum"("ws_ext_sales_price"), 2) "sales"
   , "round"("sum"("ws_net_profit"), 2) "profit"
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE ("ws_sold_date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND ("ws_web_page_sk" = "wp_web_page_sk")
   GROUP BY "wp_web_page_sk"
) 
, wr AS (
   SELECT
     "wp_web_page_sk"
   , "round"("sum"("wr_return_amt"), 2) "returns"
   , "round"("sum"("wr_net_loss"), 2) "profit_loss"
   FROM
     web_returns
   , date_dim
   , web_page
   WHERE ("wr_returned_date_sk" = "d_date_sk")
      AND ("d_date" BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND ("wr_web_page_sk" = "wp_web_page_sk")
   GROUP BY "wp_web_page_sk"
) 
SELECT
  "channel"
, "id"
, "round"("sum"("sales"), 2) "sales"
, "round"("sum"("returns"), 2) "returns"
, "round"("sum"("profit"), 2) "profit"
FROM
  (
   SELECT
     'store channel' "channel"
   , "ss"."s_store_sk" "id"
   , "sales"
   , COALESCE("returns", 0) "returns"
   , ("profit" - COALESCE("profit_loss", 0)) "profit"
   FROM
     (ss
   LEFT JOIN sr ON ("ss"."s_store_sk" = "sr"."s_store_sk"))
UNION ALL    SELECT
     'catalog channel' "channel"
   , "cs_call_center_sk" "id"
   , "sales"
   , "returns"
   , ("profit" - "profit_loss") "profit"
   FROM
     cs
   , cr
UNION ALL    SELECT
     'web channel' "channel"
   , "ws"."wp_web_page_sk" "id"
   , "sales"
   , COALESCE("returns", 0) "returns"
   , ("profit" - COALESCE("profit_loss", 0)) "profit"
   FROM
     (ws
   LEFT JOIN wr ON ("ws"."wp_web_page_sk" = "wr"."wp_web_page_sk"))
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY "channel" ASC, "id" ASC, "sales" ASC
LIMIT 100
