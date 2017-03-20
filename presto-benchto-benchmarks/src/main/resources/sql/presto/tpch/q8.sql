SELECT
  o_year,
  sum(CASE
      WHEN nation = 'BRAZIL'
        THEN volume
      ELSE 0
      END) / sum(volume) AS mkt_share
FROM (
       SELECT
         extract(YEAR FROM o.orderdate)     AS o_year,
         l.extendedprice * (1 - l.discount) AS volume,
         n2.name                          AS nation
       FROM
         "${database}"."${schema}"."part" AS p,
         "${database}"."${schema}"."supplier" AS s,
         "${database}"."${schema}"."lineitem" AS l,
         "${database}"."${schema}"."orders" AS o,
         "${database}"."${schema}"."customer" AS c,
         "${database}"."${schema}"."nation" AS n1,
         "${database}"."${schema}"."nation" AS n2,
         "${database}"."${schema}"."region" AS r
       WHERE
         p.partkey = l.partkey
         AND s.suppkey = l.suppkey
         AND l.orderkey = o.orderkey
         AND o.custkey = c.custkey
         AND c.nationkey = n1.nationkey
         AND n1.regionkey = r.regionkey
         AND r.name = 'AMERICA'
         AND s.nationkey = n2.nationkey
         AND o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
         AND p.type = 'ECONOMY ANODIZED STEEL'
     ) AS all_nations
GROUP BY
  o_year
ORDER BY
  o_year
;
