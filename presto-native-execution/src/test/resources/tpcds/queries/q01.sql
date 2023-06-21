WITH
  customer_total_return AS (
   SELECT
     "sr_customer_sk" "ctr_customer_sk"
   , "sr_store_sk" "ctr_store_sk"
   , "sum"("sr_return_amt") "ctr_total_return"
   FROM
     ${database}.${schema}.store_returns
   , ${database}.${schema}.date_dim
   WHERE ("sr_returned_date_sk" = "d_date_sk")
      AND ("d_year" = 2000)
   GROUP BY "sr_customer_sk", "sr_store_sk"
) 
SELECT "c_customer_id"
FROM
  customer_total_return ctr1
, ${database}.${schema}.store
, ${database}.${schema}.customer
WHERE ("ctr1"."ctr_total_return" > (
      SELECT ("avg"("ctr_total_return") * DECIMAL '1.2')
      FROM
        customer_total_return ctr2
      WHERE ("ctr1"."ctr_store_sk" = "ctr2"."ctr_store_sk")
   ))
   AND ("s_store_sk" = "ctr1"."ctr_store_sk")
   AND ("s_state" = 'TN')
   AND ("ctr1"."ctr_customer_sk" = "c_customer_sk")
ORDER BY "c_customer_id" ASC
LIMIT 100
