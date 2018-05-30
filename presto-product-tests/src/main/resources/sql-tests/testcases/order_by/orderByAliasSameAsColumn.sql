-- database: presto; groups: orderby
select regionkey as nationkey, nationkey as regionkey, name from tpch.tiny.nation where nationkey < 20 order by nationkey desc, regionkey asc

