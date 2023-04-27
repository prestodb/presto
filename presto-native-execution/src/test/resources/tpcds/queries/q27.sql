SELECT
  "i_item_id"
, "s_state"
, GROUPING ("s_state") "g_state"
, "round"("avg"("ss_quantity"), 2) "agg1"
, "round"("avg"("ss_list_price"), 2) "agg2"
, "round"("avg"("ss_coupon_amt"), 2) "agg3"
, "round"("avg"("ss_sales_price"), 2) "agg4"
FROM
  store_sales
, customer_demographics
, date_dim
, store
, item
WHERE ("ss_sold_date_sk" = "d_date_sk")
   AND ("ss_item_sk" = "i_item_sk")
   AND ("ss_store_sk" = "s_store_sk")
   AND ("ss_cdemo_sk" = "cd_demo_sk")
   AND ("cd_gender" = 'M')
   AND ("cd_marital_status" = 'S')
   AND ("cd_education_status" = 'College')
   AND ("d_year" = 2002)
   AND ("s_state" IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_item_id, s_state)
ORDER BY "i_item_id" ASC, "s_state" ASC
LIMIT 100
