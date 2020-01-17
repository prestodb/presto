SELECT 
  ps.partkey, 
  sum(ps.supplycost*ps.availqty) AS value
FROM 
  "${database}"."${schema}"."${prefix}partsupp" ps,
  "${database}"."${schema}"."${prefix}supplier" s,
  "${database}"."${schema}"."${prefix}nation" n
WHERE 
  ps.suppkey = s.suppkey 
  AND s.nationkey = n.nationkey 
  AND n.name = 'GERMANY'
GROUP BY 
  ps.partkey
HAVING 
  sum(ps.supplycost*ps.availqty) > (
    SELECT 
      sum(ps.supplycost*ps.availqty) * 0.0001000000
    FROM 
      "${database}"."${schema}"."${prefix}partsupp" ps,
      "${database}"."${schema}"."${prefix}supplier" s,
      "${database}"."${schema}"."${prefix}nation" n
    WHERE 
      ps.suppkey = s.suppkey 
      AND s.nationkey = n.nationkey 
      AND n.name = 'GERMANY'
  )
ORDER BY 
  value DESC
;
