-- database: presto; groups: window;
select orderkey, suppkey,
extendedprice,
round(sum(extendedprice) over (partition by suppkey order by orderkey desc rows between unbounded preceding and current row), 5) total_extendedprice,
discount,
round(avg(discount) over (partition by suppkey order by orderkey asc rows between unbounded preceding and current row), 5)  avg_discount
from tpch.tiny.lineitem where partkey = 272
