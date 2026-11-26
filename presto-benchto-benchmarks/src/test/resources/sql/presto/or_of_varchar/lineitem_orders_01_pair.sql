SELECT
  sum(l.extendedprice),
  sum(o.totalprice)
FROM
  "${database}"."${schema}"."${prefix}lineitem" l,
  "${database}"."${schema}"."${prefix}orders" o
WHERE
  l.orderkey = o.orderkey
  AND (
    (l.shipmode = 'AIR' AND l.shipinstruct = 'COLLECT COD')
  )
;
