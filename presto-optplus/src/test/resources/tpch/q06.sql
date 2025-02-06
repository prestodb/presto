--q06 database: presto; groups: tpch; tables: lineitem
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM
  lineitem
WHERE
  l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
AND l_discount BETWEEN CAST('0.06' AS DECIMAL) - CAST('0.01' AS DECIMAL) AND CAST('0.06' AS DECIMAL) + CAST('0.01' AS DECIMAL)
AND l_quantity < 24
