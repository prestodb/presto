-- database: presto; groups: tpch,quarantine; tables: lineitem,supplier
CREATE VIEW revenue (supplier_no, total_revenue) AS
  SELECT
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
  FROM
    lineitem
  WHERE
    l_shipdate >= DATE '1996-01-01'
    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
GROUP BY
  l_suppkey;

SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM
  supplier,
  revenue
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
    SELECT max(total_revenue)
    FROM
      revenue
  )
ORDER BY
  s_suppkey;

DROP VIEW revenue;