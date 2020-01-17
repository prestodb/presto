SELECT 
  cntrycode, 
  count(*) AS numcust, 
  sum(acctbal) AS totacctbal
FROM 
  (
    SELECT 
      substr(c.phone,1,2) AS cntrycode,
      c.acctbal
    FROM 
      "${database}"."${schema}"."${prefix}customer" c
    WHERE 
      substr(c.phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')
      AND c.acctbal > (
        SELECT 
          avg(c.acctbal) 
        FROM 
          "${database}"."${schema}"."${prefix}customer" c
        WHERE 
          c.acctbal > 0.00 
          AND substr(c.phone,1,2) IN ('13', '31', '23', '29', '30', '18', '17')
      ) 
      AND NOT EXISTS (
        SELECT 
          * 
        FROM 
          "${database}"."${schema}"."${prefix}orders" o
        WHERE 
          o.custkey = c.custkey
      )
  ) AS custsale
GROUP BY 
  cntrycode
ORDER BY 
  cntrycode
;
