SELECT
  "dt"."d_year"
, "item"."i_category_id"
, "item"."i_category"
, "round"("sum"("ss_ext_sales_price"), 2)
FROM
  date_dim dt
, store_sales
, item
WHERE ("dt"."d_date_sk" = "store_sales"."ss_sold_date_sk")
   AND ("store_sales"."ss_item_sk" = "item"."i_item_sk")
   AND ("item"."i_manager_id" = 1)
   AND ("dt"."d_moy" = 11)
   AND ("dt"."d_year" = 2000)
GROUP BY "dt"."d_year", "item"."i_category_id", "item"."i_category"
ORDER BY "sum"("ss_ext_sales_price") DESC, "dt"."d_year" ASC, "item"."i_category_id" ASC, "item"."i_category" ASC
LIMIT 100
