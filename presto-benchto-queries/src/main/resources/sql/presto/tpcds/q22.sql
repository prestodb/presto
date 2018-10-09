SELECT
  "i_product_name"
, "i_brand"
, "i_class"
, "i_category"
, "avg"("inv_quantity_on_hand") "qoh"
FROM
  ${database}.${schema}.inventory
, ${database}.${schema}.date_dim
, ${database}.${schema}.item
WHERE ("inv_date_sk" = "d_date_sk")
   AND ("inv_item_sk" = "i_item_sk")
   AND ("d_month_seq" BETWEEN 1200 AND (1200 + 11))
GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
ORDER BY "qoh" ASC, "i_product_name" ASC, "i_brand" ASC, "i_class" ASC, "i_category" ASC
LIMIT 100
