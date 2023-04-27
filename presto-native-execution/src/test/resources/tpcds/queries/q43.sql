SELECT
  "s_store_name"
, "s_store_id"
, "round"("sum"((CASE WHEN ("d_day_name" = 'Sunday') THEN "ss_sales_price" ELSE null END)), 2) "sun_sales"
, "round"("sum"((CASE WHEN ("d_day_name" = 'Monday') THEN "ss_sales_price" ELSE null END)), 2) "mon_sales"
, "round"("sum"((CASE WHEN ("d_day_name" = 'Tuesday') THEN "ss_sales_price" ELSE null END)), 2) "tue_sales"
, "round"("sum"((CASE WHEN ("d_day_name" = 'Wednesday') THEN "ss_sales_price" ELSE null END)), 2) "wed_sales"
, "round"("sum"((CASE WHEN ("d_day_name" = 'Thursday') THEN "ss_sales_price" ELSE null END)), 2) "thu_sales"
, "round"("sum"((CASE WHEN ("d_day_name" = 'Friday') THEN "ss_sales_price" ELSE null END)), 2) "fri_sales"
, "round"("sum"((CASE WHEN ("d_day_name" = 'Saturday') THEN "ss_sales_price" ELSE null END)), 2) "sat_sales"
FROM
  date_dim
, store_sales
, store
WHERE ("d_date_sk" = "ss_sold_date_sk")
   AND ("s_store_sk" = "ss_store_sk")
   AND ("s_gmt_offset" = -5)
   AND ("d_year" = 2000)
GROUP BY "s_store_name", "s_store_id"
ORDER BY "s_store_name" ASC, "s_store_id" ASC, "sun_sales" ASC, "mon_sales" ASC, "tue_sales" ASC, "wed_sales" ASC, "thu_sales" ASC, "fri_sales" ASC, "sat_sales" ASC
LIMIT 100
