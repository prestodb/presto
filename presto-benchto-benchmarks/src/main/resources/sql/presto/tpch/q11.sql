SELECT 
  ps.partkey, 
  sum(ps.supplycost*ps.availqty) AS value
FROM 
  "${database}"."${schema}"."partsupp" ps,
  "${database}"."${schema}"."supplier" s,
  "${database}"."${schema}"."nation" n
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
      "${database}"."${schema}"."partsupp" ps,
      "${database}"."${schema}"."supplier" s,
      "${database}"."${schema}"."nation" n
    WHERE 
      ps.suppkey = s.suppkey 
      AND s.nationkey = n.nationkey 
      AND n.name = 'GERMANY'
  )
ORDER BY 
  value DESC
;
