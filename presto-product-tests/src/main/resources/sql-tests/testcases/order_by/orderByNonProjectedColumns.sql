-- database: presto; groups: orderby
select nationkey, name from tpch.tiny.nation order by regionkey, nationkey