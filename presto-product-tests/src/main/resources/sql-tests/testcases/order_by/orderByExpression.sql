-- database: presto; groups: orderby, limit
select totalprice*1.0625, custkey from tpch.tiny.orders order by 1 limit 20