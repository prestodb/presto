SELECT
  "c_customer_id" "customer_id"
, "concat"("concat"("c_last_name", ', '), "c_first_name") "${database}.${schema}.customername"
FROM
  ${database}.${schema}.customer
, ${database}.${schema}.customer_address
, ${database}.${schema}.customer_demographics
, ${database}.${schema}.household_demographics
, ${database}.${schema}.income_band
, ${database}.${schema}.store_returns
WHERE ("ca_city" = 'Edgewood')
   AND ("c_current_addr_sk" = "ca_address_sk")
   AND ("ib_lower_bound" >= 38128)
   AND ("ib_upper_bound" <= (38128 + 50000))
   AND ("ib_income_band_sk" = "hd_income_band_sk")
   AND ("cd_demo_sk" = "c_current_cdemo_sk")
   AND ("hd_demo_sk" = "c_current_hdemo_sk")
   AND ("sr_cdemo_sk" = "cd_demo_sk")
ORDER BY "c_customer_id" ASC
LIMIT 100
