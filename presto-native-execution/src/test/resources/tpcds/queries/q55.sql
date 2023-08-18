SELECT
  "i_brand_id" "brand_id"
, "i_brand" "brand"
, "sum"("ss_ext_sales_price") "ext_price"
FROM
  ${database}.${schema}.date_dim
, ${database}.${schema}.store_sales
, ${database}.${schema}.item
WHERE ("d_date_sk" = "ss_sold_date_sk")
   AND ("ss_item_sk" = "i_item_sk")
   AND ("i_manager_id" = 28)
   AND ("d_moy" = 11)
   AND ("d_year" = 1999)
GROUP BY "i_brand", "i_brand_id"
ORDER BY "ext_price" DESC, "i_brand_id" ASC
LIMIT 100
