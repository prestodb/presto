SELECT
  count(orderkey),
  count(partkey),
  count(suppkey),
  count(linenumber),
  count(quantity),
  count(extendedprice)
FROM (
    SELECT *
    FROM ${database}.${schema}.lineitem
    ORDER BY orderkey)
