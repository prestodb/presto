SELECT 
  c_count, 
  count(*) as custdist
FROM (
  SELECT 
    c.custkey, 
    count(o.orderkey)
  FROM 
    "${database}"."${schema}"."${prefix}customer" c
    LEFT OUTER JOIN
    "${database}"."${schema}"."${prefix}orders" o
  ON 
    c.custkey = o.custkey
    AND o.comment NOT LIKE '%special%requests%'
  GROUP BY c.custkey
) AS c_orders (c_custkey, c_count)
GROUP BY 
  c_count
ORDER BY 
  custdist DESC, 
  c_count DESC
;
