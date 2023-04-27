SELECT
  "i_item_id"
, "round"("avg"("ss_quantity"), 4) "agg1"
, "round"("avg"("ss_list_price"), 2) "agg2"
, "round"("avg"("ss_coupon_amt"), 4) "agg3"
, "round"("avg"("ss_sales_price"), 2) "agg4"
FROM
  store_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE ("ss_sold_date_sk" = "d_date_sk")
   AND ("ss_item_sk" = "i_item_sk")
   AND ("ss_cdemo_sk" = "cd_demo_sk")
   AND ("ss_promo_sk" = "p_promo_sk")
   AND ("cd_gender" = 'M')
   AND ("cd_marital_status" = 'S')
   AND ("cd_education_status" = 'College')
   AND (("p_channel_email" = 'N')
      OR ("p_channel_event" = 'N'))
   AND ("d_year" = 2000)
GROUP BY "i_item_id"
ORDER BY "i_item_id" ASC
LIMIT 100
