--q01 database: presto; groups: tpch; tables: lineitem
SELECT
  l_returnflag,
  l_linestatus,
  sum(l_quantity)                                       AS sum_qty,
  sum(l_extendedprice)                                  AS sum_base_price,
  sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
  CAST(avg(l_quantity) AS DOUBLE)                       AS avg_qty,
  CAST(avg(l_extendedprice) AS DOUBLE)                  AS avg_price,
  CAST(avg(l_discount) AS DOUBLE)                       AS avg_disc,
  count(*)                                              AS count_order
FROM
  lineitem
WHERE
  l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
l_returnflag,
l_linestatus
ORDER BY
l_returnflag,
l_linestatus
