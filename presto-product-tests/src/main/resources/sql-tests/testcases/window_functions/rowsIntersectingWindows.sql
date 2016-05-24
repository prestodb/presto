-- database: presto; groups: window;
select orderkey, suppkey,
quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between 3 preceding and 1 following), 5) total_quantity,
extendedprice,
round(sum(extendedprice) over (partition by suppkey order by orderkey rows between 1 preceding and 2 following), 5)
total_extendedprice,
discount,
round(avg(discount) over (partition by suppkey order by orderkey rows between current row and unbounded following), 5)  avg_discount
from tpch.tiny.lineitem where partkey = 272
