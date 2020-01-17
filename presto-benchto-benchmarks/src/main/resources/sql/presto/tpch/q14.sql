SELECT 100.00 * sum(CASE
                    WHEN p.type LIKE 'PROMO%'
                      THEN l.extendedprice * (1 - l.discount)
                    ELSE 0
                    END) / sum(l.extendedprice * (1 - l.discount)) AS promo_revenue
FROM
  "${database}"."${schema}"."${prefix}lineitem" AS l,
  "${database}"."${schema}"."${prefix}part" AS p
WHERE
  l.partkey = p.partkey
  AND l.shipdate >= DATE '1995-09-01'
  AND l.shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
;
