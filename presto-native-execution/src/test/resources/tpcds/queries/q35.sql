SELECT
  "ca_state"
, "cd_gender"
, "cd_marital_status"
, "cd_dep_count"
, "count"(*) "cnt1"
, "min"("cd_dep_count")
, "max"("cd_dep_count")
, "avg"("cd_dep_count")
, "cd_dep_employed_count"
, "count"(*) "cnt2"
, "min"("cd_dep_employed_count")
, "max"("cd_dep_employed_count")
, "avg"("cd_dep_employed_count")
, "cd_dep_college_count"
, "count"(*) "cnt3"
, "min"("cd_dep_college_count")
, "max"("cd_dep_college_count")
, "avg"("cd_dep_college_count")
FROM
  ${database}.${schema}.customer c
, ${database}.${schema}.customer_address ca
, ${database}.${schema}.customer_demographics
WHERE ("c"."c_current_addr_sk" = "ca"."ca_address_sk")
   AND ("cd_demo_sk" = "c"."c_current_cdemo_sk")
   AND (EXISTS (
   SELECT *
   FROM
     ${database}.${schema}.store_sales
   , ${database}.${schema}.date_dim
   WHERE ("c"."c_customer_sk" = "ss_customer_sk")
      AND ("ss_sold_date_sk" = "d_date_sk")
      AND ("d_year" = 2002)
      AND ("d_qoy" < 4)
))
   AND ((EXISTS (
      SELECT *
      FROM
        ${database}.${schema}.web_sales
      , ${database}.${schema}.date_dim
      WHERE ("c"."c_customer_sk" = "ws_bill_customer_sk")
         AND ("ws_sold_date_sk" = "d_date_sk")
         AND ("d_year" = 2002)
         AND ("d_qoy" < 4)
   ))
      OR (EXISTS (
      SELECT *
      FROM
        ${database}.${schema}.catalog_sales
      , ${database}.${schema}.date_dim
      WHERE ("c"."c_customer_sk" = "cs_ship_customer_sk")
         AND ("cs_sold_date_sk" = "d_date_sk")
         AND ("d_year" = 2002)
         AND ("d_qoy" < 4)
   )))
GROUP BY "ca_state", "cd_gender", "cd_marital_status", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count"
ORDER BY "ca_state" ASC, "cd_gender" ASC, "cd_marital_status" ASC, "cd_dep_count" ASC, "cd_dep_employed_count" ASC, "cd_dep_college_count" ASC
LIMIT 100
