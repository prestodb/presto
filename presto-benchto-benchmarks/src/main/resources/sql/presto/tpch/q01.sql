SELECT
  l.returnflag,
  l.linestatus,
  sum(l.quantity)                                       AS sum_qty,
  sum(l.extendedprice)                                  AS sum_base_price,
  sum(l.extendedprice * (1 - l.discount))               AS sum_disc_price,
  sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,
  avg(l.quantity)                                       AS avg_qty,
  avg(l.extendedprice)                                  AS avg_price,
  avg(l.discount)                                       AS avg_disc,
  count(*)                                              AS count_order
FROM
  "${database}"."${schema}"."${prefix}lineitem" AS l
WHERE
  l.shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
  l.returnflag,
  l.linestatus
ORDER BY
  l.returnflag,
  l.linestatus
;
