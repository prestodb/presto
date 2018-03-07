SELECT
  "sum"("ss_net_profit") "total_sum"
, "s_state"
, "s_county"
, (GROUPING ("s_state") + GROUPING ("s_county")) "lochierarchy"
, "rank"() OVER (PARTITION BY (GROUPING ("s_state") + GROUPING ("s_county")), (CASE WHEN (GROUPING ("s_county") = 0) THEN "s_state" END) ORDER BY "sum"("ss_net_profit") DESC) "rank_within_parent"
FROM
  ${database}.${schema}.store_sales
, ${database}.${schema}.date_dim d1
, ${database}.${schema}.store
WHERE ("d1"."d_month_seq" BETWEEN 1200 AND (1200 + 11))
   AND ("d1"."d_date_sk" = "ss_sold_date_sk")
   AND ("s_store_sk" = "ss_store_sk")
   AND ("s_state" IN (
   SELECT "s_state"
   FROM
     (
      SELECT
        "s_state" "s_state"
      , "rank"() OVER (PARTITION BY "s_state" ORDER BY "sum"("ss_net_profit") DESC) "ranking"
      FROM
        ${database}.${schema}.store_sales
      , ${database}.${schema}.store
      , ${database}.${schema}.date_dim
      WHERE ("d_month_seq" BETWEEN 1200 AND (1200 + 11))
         AND ("d_date_sk" = "ss_sold_date_sk")
         AND ("s_store_sk" = "ss_store_sk")
      GROUP BY "s_state"
   )  tmp1
   WHERE ("ranking" <= 5)
))
GROUP BY ROLLUP (s_state, s_county)
ORDER BY "lochierarchy" DESC, (CASE WHEN ("lochierarchy" = 0) THEN "s_state" END) ASC, "rank_within_parent" ASC
LIMIT 100
