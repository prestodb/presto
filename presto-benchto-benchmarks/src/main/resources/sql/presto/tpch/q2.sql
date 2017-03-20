SELECT
  s.acctbal,
  s.name,
  n.name,
  p.partkey,
  p.mfgr,
  s.address,
  s.phone,
  s.comment
FROM
  "${database}"."${schema}"."part" p,
  "${database}"."${schema}"."supplier" s,
  "${database}"."${schema}"."partsupp" ps,
  "${database}"."${schema}"."nation" n,
  "${database}"."${schema}"."region" r
WHERE
  p.partkey = ps.partkey
  AND s.suppkey = ps.suppkey
  AND p.size = 15
  AND p.type like '%BRASS'
  AND s.nationkey = n.nationkey
  AND n.regionkey = r.regionkey
  AND r.name = 'EUROPE'
  AND ps.supplycost = (
    SELECT
      min(ps.supplycost)
    FROM
      "${database}"."${schema}"."partsupp" ps,
      "${database}"."${schema}"."supplier" s,
      "${database}"."${schema}"."nation" n,
      "${database}"."${schema}"."region" r
    WHERE
      p.partkey = ps.partkey
      AND s.suppkey = ps.suppkey
      AND s.nationkey = n.nationkey
      AND n.regionkey = r.regionkey
      AND r.name = 'EUROPE'
  )
ORDER BY
  s.acctbal desc,
  n.name,
  s.name,
  p.partkey
;
