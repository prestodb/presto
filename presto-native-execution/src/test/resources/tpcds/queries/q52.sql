SELECT
  "dt"."d_year"
, "item"."i_brand_id" "brand_id"
, "item"."i_brand" "brand"
, "sum"("ss_ext_sales_price") "ext_price"
FROM
  ${database}.${schema}.date_dim dt
, ${database}.${schema}.store_sales
, ${database}.${schema}.item
WHERE ("dt"."d_date_sk" = "store_sales"."ss_sold_date_sk")
   AND ("store_sales"."ss_item_sk" = "item"."i_item_sk")
   AND ("item"."i_manager_id" = 1)
   AND ("dt"."d_moy" = 11)
   AND ("dt"."d_year" = 2000)
GROUP BY "dt"."d_year", "item"."i_brand", "item"."i_brand_id"
ORDER BY "dt"."d_year" ASC, "ext_price" DESC, "brand_id" ASC
LIMIT 100
