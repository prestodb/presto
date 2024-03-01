-- database: presto; groups: orderby, distinct
select distinct brand from tpch.tiny.part where partkey < 15 order by 1 desc
