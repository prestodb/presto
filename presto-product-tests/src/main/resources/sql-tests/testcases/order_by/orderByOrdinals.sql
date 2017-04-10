-- database: presto; groups: orderby
select regionkey, nationkey from tpch.tiny.nation order by 1, 2
