SELECT 
  sum(l.extendedprice*l.discount) AS revenue
FROM 
  "${database}"."${schema}"."${prefix}lineitem" l
WHERE 
  l.shipdate >= DATE '1994-01-01'
  AND l.shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
  AND l.discount BETWEEN .06 - 0.01 AND .06 + 0.01
  AND l.quantity < 24
;
