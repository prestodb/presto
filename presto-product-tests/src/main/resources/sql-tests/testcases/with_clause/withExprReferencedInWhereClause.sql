-- database: presto; groups: with_clause; tables: nation,region; queryType: SELECT
WITH wregion AS (select min(n_regionkey) from nation where n_name >= 'N')
select r_regionkey, r_name from region where r_regionkey IN (SELECT * FROM wregion)
