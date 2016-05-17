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
        Cost: CPU 2.06ms, Input: 0 lines (0B), Output: 0 lines (0B)
        Output layout: [count]
        Output partitioning: SINGLE []
        - Output[Query Plan] => [count:bigint]
                Cost: ?, Output: 0 lines (0B)
                Query Plan := count
            - RemoteSource[2] => [count:bigint]
                    Cost: ?, Output: 0 lines (0B)

    Fragment 2 [HASH]
        Cost: CPU 700.00us, Input: 0 lines (0B), Output: 0 lines (0B)
        Output layout: [count]
        Output partitioning: SINGLE []
        - ScanFilterAndProject[] => [count:bigint]
                Cost: ?, Input: 0 lines (0B), Output: 0 lines (0B), Filtered: ?
            - Aggregate(FINAL)[clerk] => [clerk:varchar, $hashvalue:bigint, count:bigint]
                    Cost: ?, Output: 0 lines (0B)
                    count := "count"("count_8")
                - RemoteSource[3] => [clerk:varchar, count_8:bigint, $hashvalue:bigint]
                        Cost: ?, Output: 0 lines (0B)

    Fragment 3 [tpch:orders:150000]
        Cost: CPU 4.04s, Input: 29493 lines (835.29kB), Output: 0 lines (0B)
        Output layout: [clerk, count_8, $hashvalue_9]
        Output partitioning: HASH [clerk]
        - Aggregate(PARTIAL)[clerk] => [clerk:varchar, $hashvalue_9:bigint, count_8:bigint]
                Cost: 15.99%, Output: 0 lines (0B)
                count_8 := "count"(*)
            - ScanFilterAndProject[table = tpch:tpch:orders:sf0.1, originalConstraint = true] => [clerk:varchar, $hashvalue_9:bigint]
                    Cost: 84.01%, Input: 29493 lines (0B), Output: 29493 lines (835.29kB), Filtered: 0.00%
                    $hashvalue_9 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("clerk"), 0))
                    clerk := tpch:clerk

