-- database: presto; groups: window;
select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows unbounded preceding), 5) total_quantity
from tpch.tiny.lineitem where partkey = 272
