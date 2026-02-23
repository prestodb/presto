===============
EXPLAIN ANALYZE
===============

Synopsis
--------

.. code-block:: none

    EXPLAIN ANALYZE [VERBOSE] [(format <TEXT|JSON>)] statement

Description
-----------

Execute the statement and show the distributed execution plan of the statement
along with the cost of each operation.

The ``VERBOSE`` option will give more detailed information and low-level statistics;
understanding these may require knowledge of Presto internals and implementation details.
The format of the output can be set by the user with the ``format`` option. The default
output format is ``TEXT``.

.. note::

    The stats may not be entirely accurate, especially for queries that complete quickly.

Examples
--------

In the example below, you can see the CPU time spent in each stage, as well as the relative
cost of each plan node in the stage. Note that the relative cost of the plan nodes is based on
wall time, which may or may not be correlated to CPU time. For each plan node you can see
some additional statistics (e.g: average input per node instance, average number of hash collisions for
relevant plan nodes). Such statistics are useful when one wants to detect data anomalies for a query
(skewness, abnormal hash collisions).

.. code-block:: none

    presto:tiny> EXPLAIN ANALYZE SELECT
          ->     s.acctbal,
          ->     s.name,
          ->     n.name,
          ->     p.partkey,
          ->     p.mfgr,
          ->     s.address,
          ->     s.phone,
          ->     s.comment
          -> FROM
          ->     part p,
          ->     supplier s,
          ->     partsupp ps,
          ->     nation n,
          ->     region r
          -> WHERE
          ->     p.partkey = ps.partkey
          ->   AND s.suppkey = ps.suppkey
          ->   AND p.size = 15
          ->   AND p.type like '%BRASS'
          ->   AND s.nationkey = n.nationkey
          ->   AND n.regionkey = r.regionkey
          ->   AND r.name = 'EUROPE'
          ->   AND ps.supplycost = (
          ->     SELECT
          ->         min(ps.supplycost)
          ->     FROM
          ->         partsupp ps,
          ->         supplier s,
          ->         nation n,
          ->         region r
          ->     WHERE
          ->         p.partkey = ps.partkey
          ->       AND s.suppkey = ps.suppkey
          ->       AND s.nationkey = n.nationkey
          ->       AND n.regionkey = r.regionkey
          ->       AND r.name = 'EUROPE'
          -> )
          -> ORDER BY
          ->     s.acctbal desc,
          ->     n.name,
          ->     s.name,
          ->     p.partkey
          ->     LIMIT 100;



                                              Query Plan
    -----------------------------------------------------------------------------------------------
    ...
    Fragment 4 [SOURCE]
     CPU: 31.55ms, Scheduled: 38.34ms, Input: 8,020 rows (260B); per task: avg.: 8,020.00 std.dev.: 0.00, Output: 1,196 rows (21.02kB), 1 tasks
     Output layout: [partkey_15, min_73]
     Output partitioning: HASH [partkey_15]
     Output encoding: COLUMNAR
     Stage Execution Strategy: UNGROUPED_EXECUTION
     - Aggregate(PARTIAL)[partkey_15][PlanNodeId 3023] => [partkey_15:bigint, min_73:double]
             CPU: 3.00ms (1.74%), Scheduled: 4.00ms (0.54%), Output: 1,196 rows (21.02kB)
             Input total: 1,600 rows (40.63kB), avg.: 400.00 rows, std.dev.: 0.00%
             Collisions avg.: 4.50 (160.41% est.), Collisions std.dev.: 86.78%
             min_73 := "presto.default.min"((supplycost_18)) (1:365)
         - InnerJoin[PlanNodeId 2455][("suppkey_16" = "suppkey_21")] => [partkey_15:bigint, supplycost_18:double]
                 Estimates: {source: CostBasedSourceInfo, rows: 1,600 (28.13kB), cpu: 684,460.00, memory: 225.00, network: 234.00}
                 CPU: 11.00ms (6.40%), Scheduled: 13.00ms (1.77%), Output: 1,600 rows (40.63kB)
                 Left (probe) Input total: 8,000 rows (210.94kB), avg.: 2,000.00 rows, std.dev.: 0.00%
                 Right (build) Input total: 20 rows (260B), avg.: 1.25 rows, std.dev.: 60.00%
                         Collisions avg.: 0.40 (30.84% est.), Collisions std.dev.: 183.71%
                 Distribution: REPLICATED
             - ScanFilter[PlanNodeId 9,2699][table = TableHandle {connectorId='tpch', connectorHandle='partsupp:sf0.01', layout='Optional[partsupp:sf0.01]'}, grouped = false, filterPredicate = (not(IS_NULL(partkey_15))) AND (not(IS_NULL(suppkey_16)))] => [partkey_15:bigint, suppkey_16:bigint, supplycost_18:double]
                     Estimates: {source: CostBasedSourceInfo, rows: 8,000 (210.94kB), cpu: 216,000.00, memory: 0.00, network: 0.00}/{source: CostBasedSourceInfo, rows: 8,000 (210.94kB), cpu: 432,000.00, memory: 0.00, network: 0.00}
                     CPU: 14.00ms (8.14%), Scheduled: 16.00ms (2.17%), Output: 8,000 rows (210.94kB)
                     Input total: 8,000 rows (0B), avg.: 2,000.00 rows, std.dev.: 0.00%
                     partkey_15 := tpch:partkey (1:389)
                     supplycost_18 := tpch:supplycost (1:389)
                     suppkey_16 := tpch:suppkey (1:389)
                     Input: 8,000 rows (0B), Filtered: 0.00%
             - LocalExchange[PlanNodeId 2949][HASH] (suppkey_21) => [suppkey_21:bigint]
                     Estimates: {source: CostBasedSourceInfo, rows: 20 (180B), cpu: 7,480.00, memory: 54.00, network: 234.00}
                     CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Output: 20 rows (260B)
                     Input total: 20 rows (260B), avg.: 1.25 rows, std.dev.: 225.39%
                 - RemoteSource[5]  => [suppkey_21:bigint]
                         CPU: 0.00ns (0.00%), Scheduled: 0.00ns (0.00%), Output: 20 rows (260B)
                         Input total: 20 rows (260B), avg.: 1.25 rows, std.dev.: 225.39%
    ...


When the ``VERBOSE`` option is used, some operators may report additional information.
For example, the window function operator will output the following:

.. code-block:: none

    EXPLAIN ANALYZE VERBOSE SELECT count(clerk) OVER() FROM orders WHERE orderdate > date '1995-01-01';

                                              Query Plan
    -----------------------------------------------------------------------------------------------
      ...
             - Window[] => [clerk:varchar(15), count:bigint]
                     Cost: {rows: ?, bytes: ?}
                     CPU fraction: 75.93%, Output: 8130 rows (230.24kB)
                     Input avg.: 8130.00 lines, Input std.dev.: 0.00%
                     Active Drivers: [ 1 / 1 ]
                     Index size: std.dev.: 0.00 bytes , 0.00 rows
                     Index count per driver: std.dev.: 0.00
                     Rows per driver: std.dev.: 0.00
                     Size of partition: std.dev.: 0.00
                     count := count("clerk")
     ...


See Also
--------

:doc:`explain`
