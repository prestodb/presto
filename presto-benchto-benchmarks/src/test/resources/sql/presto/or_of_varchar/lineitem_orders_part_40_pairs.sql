SELECT
  sum(l.extendedprice),
  sum(o.totalprice)
FROM
  "${database}"."${schema}"."${prefix}lineitem" l,
  "${database}"."${schema}"."${prefix}orders" o,
  "${database}"."${schema}"."${prefix}part" p
WHERE
  l.orderkey = o.orderkey
  AND l.partkey = p.partkey
  AND (
    (p.brand = 'Brand#11' AND p.container = 'SM CASE')
    OR (p.brand = 'Brand#11' AND p.container = 'SM BOX')
    OR (p.brand = 'Brand#12' AND p.container = 'SM CASE')
    OR (p.brand = 'Brand#11' AND p.container = 'SM BAG')
    OR (p.brand = 'Brand#12' AND p.container = 'SM BOX')
    OR (p.brand = 'Brand#13' AND p.container = 'SM CASE')
    OR (p.brand = 'Brand#11' AND p.container = 'SM JAR')
    OR (p.brand = 'Brand#12' AND p.container = 'SM BAG')
    OR (p.brand = 'Brand#13' AND p.container = 'SM BOX')
    OR (p.brand = 'Brand#14' AND p.container = 'SM CASE')
    OR (p.brand = 'Brand#11' AND p.container = 'SM PKG')
    OR (p.brand = 'Brand#12' AND p.container = 'SM JAR')
    OR (p.brand = 'Brand#13' AND p.container = 'SM BAG')
    OR (p.brand = 'Brand#14' AND p.container = 'SM BOX')
    OR (p.brand = 'Brand#15' AND p.container = 'SM CASE')
    OR (p.brand = 'Brand#11' AND p.container = 'SM PACK')
    OR (p.brand = 'Brand#12' AND p.container = 'SM PKG')
    OR (p.brand = 'Brand#13' AND p.container = 'SM JAR')
    OR (p.brand = 'Brand#14' AND p.container = 'SM BAG')
    OR (p.brand = 'Brand#15' AND p.container = 'SM BOX')
    OR (p.brand = 'Brand#21' AND p.container = 'SM CASE')
    OR (p.brand = 'Brand#11' AND p.container = 'SM CAN')
    OR (p.brand = 'Brand#12' AND p.container = 'SM PACK')
    OR (p.brand = 'Brand#13' AND p.container = 'SM PKG')
    OR (p.brand = 'Brand#14' AND p.container = 'SM JAR')
    OR (p.brand = 'Brand#15' AND p.container = 'SM BAG')
    OR (p.brand = 'Brand#21' AND p.container = 'SM BOX')
    OR (p.brand = 'Brand#22' AND p.container = 'SM CASE')
    OR (p.brand = 'Brand#11' AND p.container = 'SM DRUM')
    OR (p.brand = 'Brand#12' AND p.container = 'SM CAN')
    OR (p.brand = 'Brand#13' AND p.container = 'SM PACK')
    OR (p.brand = 'Brand#14' AND p.container = 'SM PKG')
    OR (p.brand = 'Brand#15' AND p.container = 'SM JAR')
    OR (p.brand = 'Brand#21' AND p.container = 'SM BAG')
    OR (p.brand = 'Brand#22' AND p.container = 'SM BOX')
    OR (p.brand = 'Brand#23' AND p.container = 'SM CASE')
    OR (p.brand = 'Brand#11' AND p.container = 'MED CASE')
    OR (p.brand = 'Brand#12' AND p.container = 'SM DRUM')
    OR (p.brand = 'Brand#13' AND p.container = 'SM CAN')
    OR (p.brand = 'Brand#14' AND p.container = 'SM PACK')
  )
;
