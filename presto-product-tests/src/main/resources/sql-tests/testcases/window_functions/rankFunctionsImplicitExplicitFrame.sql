-- database: presto; groups: window;
select orderkey, discount,
dense_rank() over (order by discount),
rank() over (order by discount range between unbounded preceding and current row)
from tpch.tiny.lineitem where partkey = 272
