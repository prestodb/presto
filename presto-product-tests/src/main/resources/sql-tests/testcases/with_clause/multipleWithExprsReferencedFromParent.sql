-- database: presto; groups: with_clause; tables: nation,region; queryType: SELECT
WITH wnation AS (SELECT n_nationkey, n_regionkey FROM nation),
wregion AS (SELECT r_regionkey, r_name FROM region)
select n.n_nationkey, r.r_regionkey from wnation n join wregion r on n.n_regionkey = r.r_regionkey
where r.r_name = 'AFRICA'
