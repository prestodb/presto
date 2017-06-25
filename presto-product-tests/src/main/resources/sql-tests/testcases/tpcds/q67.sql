-- database: presto_tpcds; groups: tpcds; requires: com.teradata.tempto.fulfillment.table.hive.tpcds.ImmutableTpcdsTablesRequirements
SELECT *
FROM
  (
   SELECT
     "i_category"
   , "i_class"
   , "i_brand"
   , "i_product_name"
   , "d_year"
   , "d_qoy"
   , "d_moy"
   , "s_store_id"
   , "sumsales"
   , "rank"() OVER (PARTITION BY "i_category" ORDER BY "sumsales" DESC) "rk"
   FROM
     (
      SELECT
        "i_category"
      , "i_class"
      , "i_brand"
      , "i_product_name"
      , "d_year"
      , "d_qoy"
      , "d_moy"
      , "s_store_id"
      , "sum"(COALESCE(("ss_sales_price" * "ss_quantity"), 0)) "sumsales"
      FROM
        store_sales
      , date_dim
      , store
      , item
      WHERE ("ss_sold_date_sk" = "d_date_sk")
         AND ("ss_item_sk" = "i_item_sk")
         AND ("ss_store_sk" = "s_store_sk")
         AND ("d_month_seq" BETWEEN 1200 AND (1200 + 11))
      GROUP BY ROLLUP (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
   )  dw1
)  dw2
WHERE ("rk" <= 100)
ORDER BY "i_category" ASC, "i_class" ASC, "i_brand" ASC, "i_product_name" ASC, "d_year" ASC, "d_qoy" ASC, "d_moy" ASC, "s_store_id" ASC, "sumsales" ASC, "rk" ASC
LIMIT 100
