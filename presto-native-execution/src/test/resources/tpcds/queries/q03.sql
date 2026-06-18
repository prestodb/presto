SELECT
  "dt"."d_year"
, "item"."i_brand_id" "brand_id"
, "item"."i_brand" "brand"
, "sum"("ss_ext_sales_price") "sum_agg"
FROM
  ${database}.${schema}.date_dim dt
, ${database}.${schema}.store_sales
, ${database}.${schema}.item
WHERE ("dt"."d_date_sk" = "store_sales"."ss_sold_date_sk")
   AND ("store_sales"."ss_item_sk" = "item"."i_item_sk")
   AND ("item"."i_manufact_id" = 128)
   AND ("dt"."d_moy" = 11)
GROUP BY "dt"."d_year", "item"."i_brand", "item"."i_brand_id"
ORDER BY "dt"."d_year" ASC, "sum_agg" DESC, "brand_id" ASC
LIMIT 100
