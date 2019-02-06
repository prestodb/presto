select l_orderkey, l_linenumber, l_partkey, l_suppkey from hive.tpch.lineitem1 where l_partkey is null and l_suppkey is null;
