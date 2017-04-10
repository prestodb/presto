-- database: presto; groups: orderby, limit
select nationkey, regionkey, name from (select regionkey, nationkey, name from tpch.tiny.nation where nationkey < 20 order by 2 desc limit 5) t order by 2, 1 asc
