select l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_comment from hive.tpch.lineitem1
where (l_partkey is null or l_partkey between 1000 and 2000 or l_partkey between 10000 and 11000)
and (l_suppkey is null or l_suppkey between 1000 and 2000 or l_suppkey between 3000 and 4000)
and (l_quantity is null or l_quantity between 5 and 10 or l_quantity between 20 and 40);
