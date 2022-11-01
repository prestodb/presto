SELECT
  "w_warehouse_name"
, "w_warehouse_sq_ft"
, "w_city"
, "w_county"
, "w_state"
, "w_country"
, "ship_carriers"
, "year"
, "round"("sum"("jan_sales"), 2) "jan_sales"
, "round"("sum"("feb_sales"), 2) "feb_sales"
, "round"("sum"("mar_sales"), 2) "mar_sales"
, "round"("sum"("apr_sales"), 2) "apr_sales"
, "round"("sum"("may_sales"), 2) "may_sales"
, "round"("sum"("jun_sales"), 2) "jun_sales"
, "round"("sum"("jul_sales"), 2) "jul_sales"
, "round"("sum"("aug_sales"), 2) "aug_sales"
, "round"("sum"("sep_sales"), 2) "sep_sales"
, "round"("sum"("oct_sales"), 2) "oct_sales"
, "round"("sum"("nov_sales"), 2) "nov_sales"
, "round"("sum"("dec_sales"), 2) "dec_sales"
, "round"("sum"(("jan_sales" / "w_warehouse_sq_ft")), 2) "jan_sales_per_sq_foot"
, "round"("sum"(("feb_sales" / "w_warehouse_sq_ft")), 2) "feb_sales_per_sq_foot"
, "round"("sum"(("mar_sales" / "w_warehouse_sq_ft")), 2) "mar_sales_per_sq_foot"
, "round"("sum"(("apr_sales" / "w_warehouse_sq_ft")), 2) "apr_sales_per_sq_foot"
, "round"("sum"(("may_sales" / "w_warehouse_sq_ft")), 2) "may_sales_per_sq_foot"
, "round"("sum"(("jun_sales" / "w_warehouse_sq_ft")), 2) "jun_sales_per_sq_foot"
, "round"("sum"(("jul_sales" / "w_warehouse_sq_ft")), 2) "jul_sales_per_sq_foot"
, "round"("sum"(("aug_sales" / "w_warehouse_sq_ft")), 2) "aug_sales_per_sq_foot"
, "round"("sum"(("sep_sales" / "w_warehouse_sq_ft")), 2) "sep_sales_per_sq_foot"
, "round"("sum"(("oct_sales" / "w_warehouse_sq_ft")), 2) "oct_sales_per_sq_foot"
, "round"("sum"(("nov_sales" / "w_warehouse_sq_ft")), 2) "nov_sales_per_sq_foot"
, "round"("sum"(("dec_sales" / "w_warehouse_sq_ft")), 2) "dec_sales_per_sq_foot"
, "round"("sum"("jan_net"), 2) "jan_net"
, "round"("sum"("feb_net"), 2) "feb_net"
, "round"("sum"("mar_net"), 2) "mar_net"
, "round"("sum"("apr_net"), 2) "apr_net"
, "round"("sum"("may_net"), 2) "may_net"
, "round"("sum"("jun_net"), 2) "jun_net"
, "round"("sum"("jul_net"), 2) "jul_net"
, "round"("sum"("aug_net"), 2) "aug_net"
, "round"("sum"("sep_net"), 2) "sep_net"
, "round"("sum"("oct_net"), 2) "oct_net"
, "round"("sum"("nov_net"), 2) "nov_net"
, "round"("sum"("dec_net"), 2) "dec_net"
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
      , "round"("sum"((CASE WHEN ("d_moy" = 1) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "jan_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 2) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "feb_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 3) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "mar_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 4) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "apr_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 5) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "may_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 6) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "jun_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 7) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "jul_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 8) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "aug_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 9) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "sep_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 10) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "oct_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 11) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "nov_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 12) THEN ("ws_ext_sales_price" * "ws_quantity") ELSE 0 END)), 2) "dec_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 1) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "jan_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 2) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "feb_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 3) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "mar_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 4) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "apr_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 5) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "may_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 6) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "jun_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 7) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2)"jul_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 8) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "aug_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 9) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "sep_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 10) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "oct_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 11) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "nov_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 12) THEN ("ws_net_paid" * "ws_quantity") ELSE 0 END)), 2) "dec_net"
      FROM
        web_sales
      , warehouse
      , date_dim
      , time_dim
      , ship_mode
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
      , "round"("sum"((CASE WHEN ("d_moy" = 1) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "jan_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 2) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "feb_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 3) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "mar_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 4) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "apr_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 5) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "may_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 6) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "jun_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 7) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "jul_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 8) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "aug_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 9) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "sep_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 10) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "oct_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 11) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "nov_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 12) THEN ("cs_sales_price" * "cs_quantity") ELSE 0 END)), 2) "dec_sales"
      , "round"("sum"((CASE WHEN ("d_moy" = 1) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "jan_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 2) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "feb_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 3) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "mar_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 4) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "apr_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 5) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "may_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 6) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "jun_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 7) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "jul_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 8) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "aug_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 9) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "sep_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 10) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "oct_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 11) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "nov_net"
      , "round"("sum"((CASE WHEN ("d_moy" = 12) THEN ("cs_net_paid_inc_tax" * "cs_quantity") ELSE 0 END)), 2) "dec_net"
      FROM
        catalog_sales
      , warehouse
      , date_dim
      , time_dim
      , ship_mode
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
