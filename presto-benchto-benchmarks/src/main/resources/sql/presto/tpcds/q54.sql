WITH
  my_customers AS (
   SELECT DISTINCT
     "c_customer_sk"
   , "c_current_addr_sk"
   FROM
     (
      SELECT
        "cs_sold_date_sk" "sold_date_sk"
      , "cs_bill_customer_sk" "customer_sk"
      , "cs_item_sk" "item_sk"
      FROM
        ${database}.${schema}.catalog_sales
UNION ALL       SELECT
        "ws_sold_date_sk" "sold_date_sk"
      , "ws_bill_customer_sk" "customer_sk"
      , "ws_item_sk" "item_sk"
      FROM
        ${database}.${schema}.web_sales
   )  cs_or_ws_sales
   , ${database}.${schema}.item
   , ${database}.${schema}.date_dim
   , ${database}.${schema}.customer
   WHERE ("sold_date_sk" = "d_date_sk")
      AND ("item_sk" = "i_item_sk")
      AND ("i_category" = 'Women')
      AND ("i_class" = 'maternity                                         ')
      AND ("c_customer_sk" = "cs_or_ws_sales"."customer_sk")
      AND ("d_moy" = 12)
      AND ("d_year" = 1998)
) 
, my_revenue AS (
   SELECT
     "c_customer_sk"
   , "sum"("ss_ext_sales_price") "revenue"
   FROM
     my_customers
   , ${database}.${schema}.store_sales
   , ${database}.${schema}.customer_address
   , ${database}.${schema}.store
   , ${database}.${schema}.date_dim
   WHERE ("c_current_addr_sk" = "ca_address_sk")
      AND ("ca_county" = "s_county")
      AND ("ca_state" = "s_state")
      AND ("ss_sold_date_sk" = "d_date_sk")
      AND ("c_customer_sk" = "ss_customer_sk")
      AND ("d_month_seq" BETWEEN (
      SELECT DISTINCT ("d_month_seq" + 1)
      FROM
        ${database}.${schema}.date_dim
      WHERE ("d_year" = 1998)
         AND ("d_moy" = 12)
   ) AND (
      SELECT DISTINCT ("d_month_seq" + 3)
      FROM
        ${database}.${schema}.date_dim
      WHERE ("d_year" = 1998)
         AND ("d_moy" = 12)
   ))
   GROUP BY "c_customer_sk"
) 
, segments AS (
   SELECT CAST(("revenue" / 50) AS INTEGER) "segment"
   FROM
     my_revenue
) 
SELECT
  "segment"
, "count"(*) "num_customers"
, ("segment" * 50) "segment_base"
FROM
  segments
GROUP BY "segment"
ORDER BY "segment" ASC, "num_customers" ASC
LIMIT 100
