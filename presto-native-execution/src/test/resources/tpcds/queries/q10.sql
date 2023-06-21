SELECT
  "cd_gender"
, "cd_marital_status"
, "cd_education_status"
, "count"(*) "cnt1"
, "cd_purchase_estimate"
, "count"(*) "cnt2"
, "cd_credit_rating"
, "count"(*) "cnt3"
, "cd_dep_count"
, "count"(*) "cnt4"
, "cd_dep_employed_count"
, "count"(*) "cnt5"
, "cd_dep_college_count"
, "count"(*) "cnt6"
FROM
  ${database}.${schema}.customer c
, ${database}.${schema}.customer_address ca
, ${database}.${schema}.customer_demographics
WHERE ("c"."c_current_addr_sk" = "ca"."ca_address_sk")
   AND ("ca_county" IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
   AND ("cd_demo_sk" = "c"."c_current_cdemo_sk")
   AND (EXISTS (
   SELECT *
   FROM
     ${database}.${schema}.store_sales
   , ${database}.${schema}.date_dim
   WHERE ("c"."c_customer_sk" = "ss_customer_sk")
      AND ("ss_sold_date_sk" = "d_date_sk")
      AND ("d_year" = 2002)
      AND ("d_moy" BETWEEN 1 AND (1 + 3))
))
   AND ((EXISTS (
      SELECT *
      FROM
        ${database}.${schema}.web_sales
      , ${database}.${schema}.date_dim
      WHERE ("c"."c_customer_sk" = "ws_bill_customer_sk")
         AND ("ws_sold_date_sk" = "d_date_sk")
         AND ("d_year" = 2002)
         AND ("d_moy" BETWEEN 1 AND (1 + 3))
   ))
      OR (EXISTS (
      SELECT *
      FROM
        ${database}.${schema}.catalog_sales
      , ${database}.${schema}.date_dim
      WHERE ("c"."c_customer_sk" = "cs_ship_customer_sk")
         AND ("cs_sold_date_sk" = "d_date_sk")
         AND ("d_year" = 2002)
         AND ("d_moy" BETWEEN 1 AND (1 + 3))
   )))
GROUP BY "cd_gender", "cd_marital_status", "cd_education_status", "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count"
ORDER BY "cd_gender" ASC, "cd_marital_status" ASC, "cd_education_status" ASC, "cd_purchase_estimate" ASC, "cd_credit_rating" ASC, "cd_dep_count" ASC, "cd_dep_employed_count" ASC, "cd_dep_college_count" ASC
LIMIT 100
