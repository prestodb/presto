-- database: presto; groups: window;
select
suppkey, orderkey, partkey,
sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row) sum_quantity_A,
lag(quantity, 1, 0.0) over (partition by partkey order by orderkey rows between UNBOUNDED preceding and CURRENT ROW) lag_quantity_B,
sum(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row) sum_discount_A

from tpch.tiny.lineitem where (partkey = 272 or partkey = 273) and suppkey > 50
