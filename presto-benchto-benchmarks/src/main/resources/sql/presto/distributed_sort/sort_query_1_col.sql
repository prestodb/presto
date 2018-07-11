SELECT
  count(orderkey)
FROM (
    SELECT *
    FROM ${database}.${schema}.lineitem
    ORDER BY orderkey)
