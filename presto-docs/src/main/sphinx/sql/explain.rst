=======
EXPLAIN
=======

Synopsis
--------

.. code-block:: none

    EXPLAIN [ ( option [, ...] ) ] statement

    where option can be one of:

        FORMAT { TEXT | GRAPHVIZ }
        TYPE { LOGICAL | DISTRIBUTED | VALIDATE }

Description
-----------

Show the logical or distributed execution plan of a statement, or validate the statement.

Examples
--------

Logical plan:

.. code-block:: none

    presto:tiny> EXPLAIN SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                                    Query Plan
    ----------------------------------------------------------------------------------------------------------
     - Output[regionkey, _col1] => [regionkey:bigint, count:bigint]
             _col1 := count
         - Exchange[GATHER] => regionkey:bigint, count:bigint
             - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                     count := "count"("count_8")
                 - Exchange[REPARTITION] => regionkey:bigint, count_8:bigint
                     - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                             count_8 := "count"(*)
                         - TableScan[tpch:tpch:nation:sf0.01, original constraint=true] => [regionkey:bigint]
                                 LAYOUT: tpch:nation:sf0.01
                                 regionkey := tpch:regionkey

Distributed plan:

.. code-block:: none

    presto:tiny> EXPLAIN (TYPE DISTRIBUTED) SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                              Query Plan
    ----------------------------------------------------------------------------------------------
     Fragment 2 [SINGLE]
         Output layout: [regionkey, count]
         - Output[regionkey, _col1] => [regionkey:bigint, count:bigint]
                 _col1 := count
             - RemoteSource[1] => [regionkey:bigint, count:bigint]

     Fragment 1 [FIXED]
         Output layout: [regionkey, count]
         - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                 count := "count"("count_8")
             - RemoteSource[0] => [regionkey:bigint, count_8:bigint]

     Fragment 0 [SOURCE]
         Output layout: [regionkey, count_8]
         Output partitioning: [regionkey]
         - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                 count_8 := "count"(*)
             - TableScan[tpch:tpch:nation:sf0.01, original constraint=true] => [regionkey:bigint]
                     LAYOUT: tpch:nation:sf0.01
                     regionkey := tpch:regionkey

Validate:

.. code-block:: none

    presto:tiny> EXPLAIN (TYPE VALIDATE) SELECT regionkey, count(*) FROM nation GROUP BY 1;
     Valid
    -------
     true

See Also
--------

:doc:`explain-analyze`
