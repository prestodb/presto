-- database: presto; groups: window;
select orderkey, suppkey, discount,
rank() over (partition by suppkey)
from tpch.tiny.lineitem where partkey = 272
