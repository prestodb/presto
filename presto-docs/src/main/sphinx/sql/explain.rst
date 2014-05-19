=======
EXPLAIN
=======

Synopsis
--------

.. code-block:: none

    EXPLAIN [ ( option [, ...] ) ] statement

    where option can be one of:

        FORMAT { TEXT | GRAPHVIZ }
        TYPE { LOGICAL | DISTRIBUTED }

Description
-----------

Show the logical or distributed execution plan of a statement.

Examples
--------

Logical plan:

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

Distributed plan:

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
