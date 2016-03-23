===============
EXPLAIN ANALYZE
===============

Synopsis
--------

.. code-block:: none

    EXPLAIN ANALYZE statement

Description
-----------

Execute the statement and show the distributed execution plan of the statement
along with the cost of each operation.

.. note::

    The stats may not be 100% accurate so cost for some part of the query may be unknown

Examples
--------

.. code-block:: none

    presto:sf1> EXPLAIN ANALYZE SELECT regionkey, count(*) FROM nation GROUP BY 1;

                                              Query Plan
    -----------------------------------------------------------------------------------------------
    Fragment 1 [HASH]
        Cost: CPU 2.33ms, Input: 5 lines (90B), Output: 5 lines (90B)
        Output layout: [regionkey, count]
        Output partitioning: SINGLE []
        - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                Cost: 50.00%, Output: 5 lines (90B)
                HashAggregationOperator := Drivers: 1, Input avg.: 5.00 lines, Input std.dev.: 0.00%
                TaskOutputOperator := Drivers: 1, Input avg.: 5.00 lines, Input std.dev.: 0.00%
                count := "count"("count_8")
            - RemoteSource[2] => [regionkey:bigint, count_8:bigint]
                    Cost: 50.00%, Output: 5 lines (90B)
                    ExchangeOperator := Drivers: 1, Input avg.: 5.00 lines, Input std.dev.: 0.00%

    Fragment 2 [SOURCE]
        Cost: CPU 3.21s, Input: 25 lines (225B), Output: 5 lines (90B)
        Output layout: [regionkey, count_8]
        Output partitioning: HASH [regionkey]
        - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                Cost: 78.26%, Output: 5 lines (90B)
                HashAggregationOperator := Drivers: 4, Input avg.: 6.25 lines, Input std.dev.: 173.21%
                PartitionedOutputOperator := Drivers: 4, Input avg.: 1.25 lines, Input std.dev.: 173.21%
                count_8 := "count"(*)
         - TableScan[tpch:tpch:nation:sf1.0, originalConstraint = true] => [regionkey:bigint]
                    Cost: 21.74%, Output: 25 lines (225B)
                    TableScanOperator := Drivers: 4, Input avg.: 6.25 lines, Input std.dev.: 173.21%
                    regionkey := tpch:regionkey

