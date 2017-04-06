SELECT
  n.name,
  sum(l.extendedprice * (1 - l.discount)) AS revenue
FROM
  "${database}"."${schema}"."${prefix}customer" AS c,
  "${database}"."${schema}"."${prefix}orders" AS o,
  "${database}"."${schema}"."${prefix}lineitem" AS l,
  "${database}"."${schema}"."${prefix}supplier" AS s,
  "${database}"."${schema}"."${prefix}nation" AS n,
  "${database}"."${schema}"."${prefix}region" AS r
WHERE
  c.custkey = o.custkey
  AND l.orderkey = o.orderkey
  AND l.suppkey = s.suppkey
  AND c.nationkey = s.nationkey
  AND s.nationkey = n.nationkey
  AND n.regionkey = r.regionkey
  AND r.name = 'ASIA'
  AND o.orderdate >= DATE '1994-01-01'
  AND o.orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
  n.name
ORDER BY
  revenue DESC
;
