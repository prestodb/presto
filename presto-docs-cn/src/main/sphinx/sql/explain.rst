=======
EXPLAIN
=======

概要
--------

.. code-block:: none

    EXPLAIN [ ( option [, ...] ) ] statement

    where option can be one of:

        FORMAT { TEXT | GRAPHVIZ }
        TYPE { LOGICAL | DISTRIBUTED }

详细介绍
-----------

展示一个statement的执行逻辑和分布。

例子
--------

预计的逻辑：

.. code-block:: none

    presto:tiny> EXPLAIN SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                              Query Plan
    ----------------------------------------------------------------------------------------------
     - Output[regionkey, _col1]
             _col1 := count
         - Aggregate[regionkey] => [regionkey:bigint, count:bigint]
                 count := "count"(*)
             - TableScan[tpch:tpch:nation:sf0.01, original constraint=true] => [regionkey:bigint]
                     regionkey := tpch:tpch:regionkey:2

预计的分布：

.. code-block:: none

    presto:tiny> EXPLAIN (TYPE DISTRIBUTED) SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                                        Query Plan
    ------------------------------------------------------------------------------------------------------------------
     - Output[regionkey, _col1]
             _col1 := count
         - Exchange[[1]] => [regionkey:bigint, count:bigint]
             - Sink[12] => [regionkey:bigint, count:bigint]
                 - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                         count := "count"("count_3")
                     - Exchange[[0]] => [regionkey:bigint, count_3:bigint]
                         - Sink[9] => [regionkey:bigint, count_3:bigint]
                             - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_3:bigint]
                                     count_3 := "count"(*)
                                 - TableScan[tpch:tpch:nation:sf0.01, original constraint=true] => [regionkey:bigint]
                                         regionkey := tpch:tpch:regionkey:2
