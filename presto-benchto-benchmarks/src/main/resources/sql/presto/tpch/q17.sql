SELECT 
  sum(l.extendedprice)/7.0 as avg_yearly 
FROM 
  "${database}"."${schema}"."${prefix}lineitem" l,
  "${database}"."${schema}"."${prefix}part" p
WHERE 
  p.partkey = l.partkey 
  AND p.brand = 'Brand#23' 
  AND p.container = 'MED BOX'
  AND l.quantity < (
    SELECT 
      0.2*avg(l.quantity) 
    FROM 
      "${database}"."${schema}"."${prefix}lineitem" l
    WHERE 
    l.partkey = p.partkey
  )
;
