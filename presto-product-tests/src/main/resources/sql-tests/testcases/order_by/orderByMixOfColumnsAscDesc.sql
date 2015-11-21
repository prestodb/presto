-- database: presto; groups: orderby, limit
select orderdate, orderpriority, custkey from tpch.tiny.orders order by 1 desc, 2, 3 desc limit 20

