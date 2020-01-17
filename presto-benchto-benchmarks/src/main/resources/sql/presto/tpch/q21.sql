SELECT 
  s.name, 
  count(*) as numwait
FROM 
  "${database}"."${schema}"."${prefix}supplier" s,
  "${database}"."${schema}"."${prefix}lineitem" l1,
  "${database}"."${schema}"."${prefix}orders" o,
  "${database}"."${schema}"."${prefix}nation" n
WHERE 
  s.suppkey = l1.suppkey 
  AND o.orderkey = l1.orderkey
  AND o.orderstatus = 'F'
  AND l1.receiptdate> l1.commitdate
  AND EXISTS (
    SELECT 
      * 
    FROM 
      "${database}"."${schema}"."${prefix}lineitem" l2
    WHERE 
      l2.orderkey = l1.orderkey
      AND l2.suppkey <> l1.suppkey
  ) 
  AND NOT EXISTS (
    SELECT 
      * 
    FROM 
      "${database}"."${schema}"."${prefix}lineitem" l3
    WHERE 
      l3.orderkey = l1.orderkey 
      AND l3.suppkey <> l1.suppkey 
      AND l3.receiptdate > l3.commitdate
  ) 
  AND s.nationkey = n.nationkey 
  AND n.name = 'SAUDI ARABIA'
GROUP BY 
  s.name
ORDER BY 
  numwait DESC, 
  s.name
LIMIT 
  100
;
