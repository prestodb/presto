SELECT
  "w_warehouse_name"
, "w_warehouse_sq_ft"
, "w_city"
, "w_county"
, "w_state"
, "w_country"
, "ship_carriers"
, "year"
, "sum"("jan_sales") "jan_sales"
, "sum"("feb_sales") "feb_sales"
, "sum"("mar_sales") "mar_sales"
, "sum"("apr_sales") "apr_sales"
, "sum"("may_sales") "may_sales"
, "sum"("jun_sales") "jun_sales"
, "sum"("jul_sales") "jul_sales"
, "sum"("aug_sales") "aug_sales"
, "sum"("sep_sales") "sep_sales"
, "sum"("oct_sales") "oct_sales"
, "sum"("nov_sales") "nov_sales"
, "sum"("dec_sales") "dec_sales"
, "sum"(("jan_sales" / "w_warehouse_sq_ft")) "jan_sales_per_sq_foot"
, "sum"(("feb_sales" / "w_warehouse_sq_ft")) "feb_sales_per_sq_foot"
, "sum"(("mar_sales" / "w_warehouse_sq_ft")) "mar_sales_per_sq_foot"
, "sum"(("apr_sales" / "w_warehouse_sq_ft")) "apr_sales_per_sq_foot"
, "sum"(("may_sales" / "w_warehouse_sq_ft")) "may_sales_per_sq_foot"
, "sum"(("jun_sales" / "w_warehouse_sq_ft")) "jun_sales_per_sq_foot"
, "sum"(("jul_sales" / "w_warehouse_sq_ft")) "jul_sales_per_sq_foot"
, "sum"(("aug_sales" / "w_warehouse_sq_ft")) "aug_sales_per_sq_foot"
, "sum"(("sep_sales" / "w_warehouse_sq_ft")) "sep_sales_per_sq_foot"
, "sum"(("oct_sales" / "w_warehouse_sq_ft")) "oct_sales_per_sq_foot"
, "sum"(("nov_sales" / "w_warehouse_sq_ft")) "nov_sales_per_sq_foot"
, "sum"(("dec_sales" / "w_warehouse_sq_ft")) "dec_sales_per_sq_foot"
, "sum"("jan_net") "jan_net"
, "sum"("feb_net") "feb_net"
, "sum"("mar_net") "mar_net"
, "sum"("apr_net") "apr_net"
, "sum"("may_net") "may_net"
, "sum"("jun_net") "jun_net"
, "sum"("jul_net") "jul_net"
, "sum"("aug_net") "aug_net"
, "sum"("sep_net") "sep_net"
, "sum"("oct_net") "oct_net"
, "sum"("nov_net") "nov_net"
, "sum"("dec_net") "dec_net"
FROM
(
      SELECT
        "w_warehouse_name"
      , "w_warehouse_sq_ft"
      , "w_city"
      , "w_county"
      , "w_state"
      , "w_country"
      , "concat"("concat"('DHL', ','), 'BARIAN') "ship_carriers"
      , "d_year" "YEAR"
      , "sum"((CASE WHEN ("d_moy" = 1) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "jan_sales"
      , "sum"((CASE WHEN ("d_moy" = 2) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "feb_sales"
      , "sum"((CASE WHEN ("d_moy" = 3) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "mar_sales"
      , "sum"((CASE WHEN ("d_moy" = 4) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "apr_sales"
      , "sum"((CASE WHEN ("d_moy" = 5) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "may_sales"
      , "sum"((CASE WHEN ("d_moy" = 6) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "jun_sales"
      , "sum"((CASE WHEN ("d_moy" = 7) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "jul_sales"
      , "sum"((CASE WHEN ("d_moy" = 8) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "aug_sales"
      , "sum"((CASE WHEN ("d_moy" = 9) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "sep_sales"
      , "sum"((CASE WHEN ("d_moy" = 10) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "oct_sales"
      , "sum"((CASE WHEN ("d_moy" = 11) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "nov_sales"
      , "sum"((CASE WHEN ("d_moy" = 12) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)) "dec_sales"
      , "sum"((CASE WHEN ("d_moy" = 1) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "jan_net"
      , "sum"((CASE WHEN ("d_moy" = 2) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "feb_net"
      , "sum"((CASE WHEN ("d_moy" = 3) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "mar_net"
      , "sum"((CASE WHEN ("d_moy" = 4) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "apr_net"
      , "sum"((CASE WHEN ("d_moy" = 5) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "may_net"
      , "sum"((CASE WHEN ("d_moy" = 6) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "jun_net"
      , "sum"((CASE WHEN ("d_moy" = 7) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "jul_net"
      , "sum"((CASE WHEN ("d_moy" = 8) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "aug_net"
      , "sum"((CASE WHEN ("d_moy" = 9) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "sep_net"
      , "sum"((CASE WHEN ("d_moy" = 10) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "oct_net"
      , "sum"((CASE WHEN ("d_moy" = 11) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "nov_net"
      , "sum"((CASE WHEN ("d_moy" = 12) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)) "dec_net"
      FROM
        ${database}.${schema}.web_sales
      , ${database}.${schema}.warehouse
      , ${database}.${schema}.date_dim
      , ${database}.${schema}.time_dim
      , ${database}.${schema}.ship_mode
      WHERE ("ws_warehouse_sk" = "w_warehouse_sk")
         AND ("ws_sold_date_sk" = "d_date_sk")
         AND ("ws_sold_time_sk" = "t_time_sk")
         AND ("ws_ship_mode_sk" = "sm_ship_mode_sk")
         AND ("d_year" = 2001)
         AND ("t_time" BETWEEN 30838 AND (30838 + 28800))
         AND ("sm_carrier" IN ('DHL'      , 'BARIAN'))
      GROUP BY "w_warehouse_name", "w_warehouse_sq_ft", "w_city", "w_county", "w_state", "w_country", "d_year"
   UNION ALL
      SELECT
        "w_warehouse_name"
      , "w_warehouse_sq_ft"
      , "w_city"
      , "w_county"
      , "w_state"
      , "w_country"
      , "concat"("concat"('DHL', ','), 'BARIAN') "ship_carriers"
      , "d_year" "YEAR"
      , "sum"((CASE WHEN ("d_moy" = 1) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "jan_sales"
      , "sum"((CASE WHEN ("d_moy" = 2) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "feb_sales"
      , "sum"((CASE WHEN ("d_moy" = 3) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "mar_sales"
      , "sum"((CASE WHEN ("d_moy" = 4) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "apr_sales"
      , "sum"((CASE WHEN ("d_moy" = 5) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "may_sales"
      , "sum"((CASE WHEN ("d_moy" = 6) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "jun_sales"
      , "sum"((CASE WHEN ("d_moy" = 7) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "jul_sales"
      , "sum"((CASE WHEN ("d_moy" = 8) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "aug_sales"
      , "sum"((CASE WHEN ("d_moy" = 9) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "sep_sales"
      , "sum"((CASE WHEN ("d_moy" = 10) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "oct_sales"
      , "sum"((CASE WHEN ("d_moy" = 11) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "nov_sales"
      , "sum"((CASE WHEN ("d_moy" = 12) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)) "dec_sales"
      , "sum"((CASE WHEN ("d_moy" = 1) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "jan_net"
      , "sum"((CASE WHEN ("d_moy" = 2) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "feb_net"
      , "sum"((CASE WHEN ("d_moy" = 3) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "mar_net"
      , "sum"((CASE WHEN ("d_moy" = 4) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "apr_net"
      , "sum"((CASE WHEN ("d_moy" = 5) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "may_net"
      , "sum"((CASE WHEN ("d_moy" = 6) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "jun_net"
      , "sum"((CASE WHEN ("d_moy" = 7) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "jul_net"
      , "sum"((CASE WHEN ("d_moy" = 8) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "aug_net"
      , "sum"((CASE WHEN ("d_moy" = 9) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "sep_net"
      , "sum"((CASE WHEN ("d_moy" = 10) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "oct_net"
      , "sum"((CASE WHEN ("d_moy" = 11) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "nov_net"
      , "sum"((CASE WHEN ("d_moy" = 12) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)) "dec_net"
      FROM
        ${database}.${schema}.catalog_sales
      , ${database}.${schema}.warehouse
      , ${database}.${schema}.date_dim
      , ${database}.${schema}.time_dim
      , ${database}.${schema}.ship_mode
      WHERE ("cs_warehouse_sk" = "w_warehouse_sk")
         AND ("cs_sold_date_sk" = "d_date_sk")
         AND ("cs_sold_time_sk" = "t_time_sk")
         AND ("cs_ship_mode_sk" = "sm_ship_mode_sk")
         AND ("d_year" = 2001)
         AND ("t_time" BETWEEN 30838 AND (30838 + 28800))
         AND ("sm_carrier" IN ('DHL'      , 'BARIAN'))
      GROUP BY "w_warehouse_name", "w_warehouse_sq_ft", "w_city", "w_county", "w_state", "w_country", "d_year"
   )  x
GROUP BY "w_warehouse_name", "w_warehouse_sq_ft", "w_city", "w_county", "w_state", "w_country", "ship_carriers", "year"
ORDER BY "w_warehouse_name" ASC
LIMIT 100
