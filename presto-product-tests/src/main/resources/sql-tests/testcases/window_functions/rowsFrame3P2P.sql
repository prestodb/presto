-- database: presto; groups: window;
select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between 3 preceding and 2 preceding), 5) total_quantity
from tpch.tiny.lineitem where partkey = 272
