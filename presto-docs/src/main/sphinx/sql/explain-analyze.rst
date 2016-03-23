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

    The stats may not be entirely accurate, especially for queries that complete quickly.

Examples
--------

In the example below, you can see the CPU time spent in each stage, as well as the relative
cost of each operator in the stage. Note that the relative cost of the operators is based on
wall time, which may or may not be correlated to CPU time.

.. code-block:: none

    presto:sf1> EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk;

                                              Query Plan
    -----------------------------------------------------------------------------------------------
    Fragment 1 [SINGLE]
        Cost: CPU 30.00us, Input: 0 lines (0B), Output: 0 lines (0B)
        Output layout: [count]
        Output partitioning: SINGLE []
        - Output[Query Plan] => [count:bigint]
                Cost: ?%, Output: 0 lines (0B)
                TaskOutputOperator := Drivers: 1, Input avg.: 0.00 lines, Input std.dev.: ?%
                TaskOutputOperator := Collisions avg.: ? (?% est.), Collisions std.dev.: ?%
                Query Plan := count
            - RemoteSource[2] => [count:bigint]
                    Cost: ?%, Output: 0 lines (0B)
                    ExchangeOperator := Drivers: 1, Input avg.: 0.00 lines, Input std.dev.: ?%
                    ExchangeOperator := Collisions avg.: ? (?% est.), Collisions std.dev.: ?%

    Fragment 2 [HASH]
        Cost: CPU 40.00us, Input: 0 lines (0B), Output: 0 lines (0B)
        Output layout: [count]
        Output partitioning: SINGLE []
        - ScanFilterAndProject[] => [count:bigint]
                Cost: ?%, Input: 0 lines (0B), Output: 0 lines (0B), Filtered: ?%
                TaskOutputOperator := Drivers: 1, Input avg.: 0.00 lines, Input std.dev.: ?%
                TaskOutputOperator := Collisions avg.: ? (?% est.), Collisions std.dev.: ?%
                FilterAndProjectOperator := Drivers: 1, Input avg.: 0.00 lines, Input std.dev.: ?%
                FilterAndProjectOperator := Collisions avg.: ? (?% est.), Collisions std.dev.: ?%
            - Aggregate(FINAL)[clerk] => [clerk:varchar, $hashvalue:bigint, count:bigint]
                    Cost: ?%, Output: 0 lines (0B)
                    HashAggregationOperator := Drivers: 1, Input avg.: 0.00 lines, Input std.dev.: ?%
                    HashAggregationOperator := Collisions avg.: ? (?% est.), Collisions std.dev.: ?%
                    count := "count"("count_8")
                - RemoteSource[3] => [clerk:varchar, count_8:bigint, $hashvalue:bigint]
                        Cost: ?%, Output: 0 lines (0B)
                        ExchangeOperator := Drivers: 1, Input avg.: 0.00 lines, Input std.dev.: ?%
                        ExchangeOperator := Collisions avg.: ? (?% est.), Collisions std.dev.: ?%

    Fragment 3 [tpch:orders:150000]
        Cost: CPU 0.00ns, Input: 121249 lines (3.35MB), Output: 0 lines (0B)
        Output layout: [clerk, count_8, $hashvalue_9]
        Output partitioning: HASH [clerk]
        - Aggregate(PARTIAL)[clerk] => [clerk:varchar, $hashvalue_9:bigint, count_8:bigint]
                Cost: 4.63%, Output: 0 lines (0B)
                HashAggregationOperator := Drivers: 4, Input avg.: 29493.00 lines, Input std.dev.: 0.00%
                HashAggregationOperator := Collisions avg.: 808.50 (2491.60% est.), Collisions std.dev.: 4.01%
                PartitionedOutputOperator := Drivers: 4, Input avg.: 0.00 lines, Input std.dev.: ?%
                PartitionedOutputOperator := Collisions avg.: ? (?% est.), Collisions std.dev.: ?%
                count_8 := "count"(*)
            - ScanFilterAndProject[table = tpch:tpch:orders:sf0.1, originalConstraint = true] => [clerk:varchar, $hashvalue_9:bigint]
                    Cost: 95.37%, Input: 121249 lines (0B), Output: 121249 lines (3.35MB), Filtered: 0.00%
                    ScanFilterAndProjectOperator := Drivers: 4, Input avg.: 30312.25 lines, Input std.dev.: 4.68%
                    $hashvalue_9 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("clerk"), 0))
                    clerk := tpch:clerk
 