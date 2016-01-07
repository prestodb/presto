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
    Also, queries that have a ``Project`` or ``Filter`` above a ``TableScan`` are always
    inaccurate at this time (the cost will be attributed to a single node).

Examples
--------

In the example below, you can see the CPU time spent in each stage, as well as the relative
cost of each operator in the stage. Note that the relative cost of the operators is based on
wall time, which may or may not be correlated to CPU time.

.. code-block:: none

    presto:sf1> EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk;

                                              Query Plan
    -----------------------------------------------------------------------------------------------
    Fragment 1 [HASH]
        Cost: CPU 111.00us, Input 1000 (37.11kB), Output 10 (160B)
        Output layout: [clerk, $hashvalue, count]
        Output partitioning: SINGLE []
        - Aggregate(FINAL)[clerk] => [clerk:varchar, $hashvalue:bigint, count:bigint]
                Cost: 0.00%, Output 0 (0B)
                count := "count"("count_8")
            - RemoteSource[2] => [clerk:varchar, $hashvalue:bigint, count_8:bigint]
                    Cost: 100.00%, Output 1000 (37.11kB)

    Fragment 2 [SOURCE]
        Cost: CPU 8.38s, Input 1500000 (41.49MB), Output 4000 (148.45kB)
        Output layout: [clerk, $hashvalue, count_8]
        Output partitioning: HASH [clerk]
        - Aggregate(PARTIAL)[clerk] => [clerk:varchar, $hashvalue:bigint, count_8:bigint]
                Cost: 7.45%, Output 4000 (148.45kB)
                count_8 := "count"(*)
            - Project => [clerk:varchar, $hashvalue:bigint]
                    Cost: 92.55%, Output 1500000 (41.49MB)
                    $hashvalue := "combine_hash"(0, COALESCE("$operator$hash_code"("clerk"), 0))
                - TableScan[tpch:tpch:orders:sf1.0, originalConstraint = true] => [clerk:varchar]
                        Cost: unknown, Output: unknown
                        clerk := tpch:clerk

