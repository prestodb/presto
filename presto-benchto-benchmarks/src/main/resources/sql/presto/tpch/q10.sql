SELECT
  c.custkey,
  c.name,
  sum(l.extendedprice * (1 - l.discount)) AS revenue,
  c.acctbal,
  n.name,
  c.address,
  c.phone,
  c.comment
FROM
  "${database}"."${schema}"."${prefix}lineitem" AS l,
  "${database}"."${schema}"."${prefix}orders" AS o,
  "${database}"."${schema}"."${prefix}customer" AS c,
  "${database}"."${schema}"."${prefix}nation" AS n
WHERE
  c.custkey = o.custkey
  AND l.orderkey = o.orderkey
  AND o.orderdate >= DATE '1993-10-01'
  AND o.orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
  AND l.returnflag = 'R'
  AND c.nationkey = n.nationkey
GROUP BY
  c.custkey,
  c.name,
  c.acctbal,
  c.phone,
  n.name,
  c.address,
  c.comment
ORDER BY
  revenue DESC
LIMIT 20
;
