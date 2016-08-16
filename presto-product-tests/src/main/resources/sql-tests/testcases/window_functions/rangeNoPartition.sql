-- database: presto; groups: window;
select orderkey, discount, extendedprice,
min(extendedprice) over (order by discount range current row) min_extendedprice,
max(extendedprice) over (order by discount range current row) max_extendedprice
from tpch.tiny.lineitem where partkey = 272
