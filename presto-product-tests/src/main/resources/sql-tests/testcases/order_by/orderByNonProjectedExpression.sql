-- database: presto; groups: orderby, limit
select custkey, orderstatus from tpch.tiny.orders order by totalprice*1.0625 limit 20