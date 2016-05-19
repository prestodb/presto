-- database: presto; groups: window;
select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between 2 following and 3 following), 5) total_quantity
from tpch.tiny.lineitem where partkey = 272
