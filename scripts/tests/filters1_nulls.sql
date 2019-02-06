
select l_orderkey, l_linenumber, l_partkey, l_suppkey, l_comment from  hive.tpch.lineitem1_nulls where l_orderkey between 100000 and 200000
  and l_partkey between 10000 and 30000
    and l_suppkey between 1000 and 5000      and l_comment > 'f';
