SELECT
  sum(l.extendedprice)
FROM
  "${database}"."${schema}"."${prefix}lineitem" l,
  "${database}"."${schema}"."${prefix}part" p
WHERE
  l.partkey = p.partkey
  AND (
    (p.brand = 'Brand#11' AND p.container = 'SM CASE')
  )
;
