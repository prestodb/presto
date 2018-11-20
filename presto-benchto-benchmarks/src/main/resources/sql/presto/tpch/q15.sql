WITH revenue0 AS (
  SELECT 
    l.suppkey as supplier_no,
    sum(l.extendedprice*(1-l.discount)) as total_revenue
  FROM 
    "${database}"."${schema}"."${prefix}lineitem" l
  WHERE 
    l.shipdate >= DATE '1996-01-01'
    AND l.shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
  GROUP BY 
    l.suppkey
)
 
/* TPC_H Query 15 - Top Supplier */
SELECT 
  s.suppkey, 
  s.name, 
  s.address, 
  s.phone, 
  total_revenue
FROM 
  "${database}"."${schema}"."${prefix}supplier" s,
  revenue0
WHERE 
  s.suppkey = supplier_no 
  AND total_revenue = (SELECT max(total_revenue) FROM revenue0)
ORDER BY 
  s.suppkey
;
