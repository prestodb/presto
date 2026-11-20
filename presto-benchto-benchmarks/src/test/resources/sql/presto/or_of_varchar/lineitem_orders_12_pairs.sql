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
    OR (l.shipmode = 'AIR' AND l.shipinstruct = 'DELIVER IN PERSON')
    OR (l.shipmode = 'FOB' AND l.shipinstruct = 'COLLECT COD')
    OR (l.shipmode = 'AIR' AND l.shipinstruct = 'NONE')
    OR (l.shipmode = 'FOB' AND l.shipinstruct = 'DELIVER IN PERSON')
    OR (l.shipmode = 'MAIL' AND l.shipinstruct = 'COLLECT COD')
    OR (l.shipmode = 'AIR' AND l.shipinstruct = 'TAKE BACK RETURN')
    OR (l.shipmode = 'FOB' AND l.shipinstruct = 'NONE')
    OR (l.shipmode = 'MAIL' AND l.shipinstruct = 'DELIVER IN PERSON')
    OR (l.shipmode = 'RAIL' AND l.shipinstruct = 'COLLECT COD')
    OR (l.shipmode = 'FOB' AND l.shipinstruct = 'TAKE BACK RETURN')
    OR (l.shipmode = 'MAIL' AND l.shipinstruct = 'NONE')
  )
;
