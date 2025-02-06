--q15 database: presto; groups: tpch; tables: lineitem,supplier

SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM
  supplier,
  (   SELECT
         l_suppkey AS supplier_no,
         sum(l_extendedprice * (1 - l_discount)) AS total_revenue
       FROM
         lineitem
       WHERE
         l_shipdate >= DATE '1996-01-01'
         AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
     GROUP BY
       l_suppkey)
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
    SELECT max(total_revenue)
    FROM
         (SELECT
            l_suppkey AS supplier_no,
            sum(l_extendedprice * (1 - l_discount)) AS total_revenue
          FROM
            lineitem
          WHERE
            l_shipdate >= DATE '1996-01-01'
            AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
        GROUP BY
          l_suppkey)
  )
ORDER BY
  s_suppkey
