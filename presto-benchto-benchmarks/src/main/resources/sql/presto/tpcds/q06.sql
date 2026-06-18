SELECT
  "a"."ca_state" "STATE"
, "count"(*) "cnt"
FROM
  ${database}.${schema}.customer_address a
, ${database}.${schema}.customer c
, ${database}.${schema}.store_sales s
, ${database}.${schema}.date_dim d
, ${database}.${schema}.item i
WHERE ("a"."ca_address_sk" = "c"."c_current_addr_sk")
   AND ("c"."c_customer_sk" = "s"."ss_customer_sk")
   AND ("s"."ss_sold_date_sk" = "d"."d_date_sk")
   AND ("s"."ss_item_sk" = "i"."i_item_sk")
   AND ("d"."d_month_seq" = (
      SELECT DISTINCT "d_month_seq"
      FROM
        ${database}.${schema}.date_dim
      WHERE ("d_year" = 2001)
         AND ("d_moy" = 1)
   ))
   AND ("i"."i_current_price" > (DECIMAL '1.2' * (
         SELECT "avg"("j"."i_current_price")
         FROM
           ${database}.${schema}.item j
         WHERE ("j"."i_category" = "i"."i_category")
      )))
GROUP BY "a"."ca_state"
HAVING ("count"(*) >= 10)
ORDER BY "cnt" ASC, "a"."ca_state" ASC
LIMIT 100
