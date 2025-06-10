==================
printPlanWithStats
==================

Velox collects a number of valuable statistics during query execution.  These
counters are exposed via Task::taskStats() API for programmatic access and can
be printed in a human-friendly format for manual inspection. We use these to
reason about the query execution dynamic and troubleshoot performance issues.

If you are familiar with Presto, the tools described below would look very
similar to the PrestoQueryLookup tool available via bunnylol presto
<query-id>.

PlanNode::toString()
--------------------

PlanNode::toString() method prints a query plan as a tree of plan nodes. This
API can be used before or after executing a query.

PlanNode::toString() method takes two optional flags: detailed and recursive.
When detailed is true, the output includes extra details about each plan node.
When recursive is true, the output includes the whole plan tree, otherwise only
a single plan node is shown.

In “detailed” mode, Project node shows projection expressions, Filter node shows
filter expression, Join node shows join type and join keys, Aggregation node
shows grouping keys and aggregate functions, OrderBy node shows sorting keys
and orders, etc.

Let’s use a simple join query as an example:

`plan->toString(false /*detailed*/, true /*recursive*/)` prints a plan tree using plan node names:

.. code-block::

    -> Project
      -> HashJoin
        -> TableScan
        -> Project
          -> Values

`plan->toString(true /*detailed*/, true /*recursive*/)` adds plan node details to each plan node.

.. code-block::

    -> Project[expressions: (c0:INTEGER, ROW["c0"]), (p1:BIGINT, plus(ROW["c1"],1)), (p2:BIGINT, plus(ROW["c1"],ROW["u_c1"])), ]
      -> HashJoin[INNER c0=u_c0]
        -> TableScan[]
        -> Project[expressions: (u_c0:INTEGER, ROW["c0"]), (u_c1:BIGINT, ROW["c1"]), ]
          -> Values[100 rows in 1 vectors]

Let’s also look at an aggregation query:

`plan->toString(false /*detailed*/, true /*recursive*/)`

.. code-block::

    -> Aggregation
      -> TableScan

`plan->toString(true /*detailed*/, true /*recursive*/)`

.. code-block::

    -> Aggregation[PARTIAL [c5] a0 := max(ROW["c0"]), a1 := sum(ROW["c1"]), a2 := sum(ROW["c2"]), a3 := sum(ROW["c3"]), a4 := sum(ROW["c4"])]
      -> TableScan[]

printPlanWithStats()
--------------------

printPlanWithStats() function prints a query plan annotated with runtime
statistics. This function can be used after the query finishes processing. It
takes a root plan node and a TaskStats struct.

By default, printPlanWithStats shows a number of output rows, CPU time, number of threads used, and peak
memory usage for each plan node.

`printPlanWithStats(*plan, task->taskStats())`

.. code-block::

     -> Project[expressions: (c0:INTEGER, ROW["c0"]), (p1:BIGINT, plus(ROW["c1"],1)), (p2:BIGINT, plus(ROW["c1"],ROW["u_c1"]))]
        Output: 2000 rows (154.98KB), Cpu time: 907.80us, Blocked wall time: 0ns, Peak memory: 1.00MB, Threads: 1, CPU breakdown: I/O/F (27.24us/872.82us/7.74us)
       -> HashJoin[INNER c0=u_c0]
          Output: 2000 rows (136.88KB), Cpu time: 508.74us, Blocked wall time: 117.00us, Peak memory: 2.00MB, CPU breakdown: I/O/F (177.87us/329.20us/1.66us)
          HashBuild: Input: 100 rows (1.31KB), Output: 0 rows (0B), Cpu time: 41.77us, Blocked wall time: 0ns, Peak memory: 1.00MB, Threads: 1, CPU breakdown: I/O/F (40.18us/1.59us/0ns)
          HashProbe: Input: 2000 rows (118.12KB), Output: 2000 rows (136.88KB), Cpu time: 466.97us, Blocked wall time: 117.00us, Peak memory: 1.00MB, Threads: 1, CPU breakdown: I/O/F (137.69us/327.61us/1.66us)
         -> TableScan[Table: Orders]
            Input: 2000 rows (118.12KB), Raw Input: 20480 rows (72.31KB), Output: 2000 rows (118.12KB), Cpu time: 8.89ms, Blocked wall time: 5.00us, Peak memory: 1.00MB, Threads: 1, Splits: 20, CPU breakdown: I/O/F (0ns/8.88ms/4.93us)
         -> Project[expressions: (u_c0:INTEGER, ROW["c0"]), (u_c1:BIGINT, ROW["c1"])]
            Output: 100 rows (1.31KB), Cpu time: 43.22us, Blocked wall time: 0ns, Peak memory: 0B, Threads: 1, CPU breakdown: I/O/F (691ns/5.54us/36.98us)
           -> Values[100 rows in 1 vectors]
              Input: 0 rows (0B), Output: 100 rows (1.31KB), Cpu time: 3.05us, Blocked wall time: 0ns, Peak memory: 0B, Threads: 1, CPU breakdown: I/O/F (0ns/2.48us/568ns)

With includeCustomStats flag enabled, printPlanWithStats adds operator-specific
statistics for each plan node, e.g. number of distinct values for the join key,
number of row groups skipped in table scan, amount of data read from cache and
storage in table scan, number of rows processed via aggregation pushdown into
scan, etc.

Here is the output for the join query from above.

`printPlanWithStats(*plan, task->taskStats(), true)` shows custom operator statistics.

.. code-block::

    -> Project[expressions: (c0:INTEGER, ROW["c0"]), (p1:BIGINT, plus(ROW["c1"],1)), (p2:BIGINT, plus(ROW["c1"],ROW["u_c1"]))]
       Output: 2000 rows (154.98KB), Cpu time: 907.80us, Blocked wall time: 0ns, Peak memory: 1.00MB, Threads: 1, CPU breakdown: I/O/F (27.24us/872.82us/7.74us)
          dataSourceLazyWallNanos    sum: 473.00us, count: 20, min: 11.00us, max: 96.00us
      -> HashJoin[INNER c0=u_c0]
         Output: 2000 rows (136.88KB), Cpu time: 508.74us, Blocked wall time: 223.00us, Peak memory: 2.00MB, CPU breakdown: I/O/F (177.87us/329.20us/1.66us)
         HashBuild: Input: 100 rows (1.31KB), Output: 0 rows (0B), Cpu time: 41.77us, Blocked wall time: 0ns, Peak memory: 1.00MB, Threads: 1, CPU breakdown: I/O/F (40.18us/1.59us/0ns)
            distinctKey0       sum: 101, count: 1, min: 101, max: 101
            queuedWallNanos    sum: 125.00us, count: 1, min: 125.00us, max: 125.00us
            rangeKey0          sum: 200, count: 1, min: 200, max: 200
         HashProbe: Input: 2000 rows (118.12KB), Output: 2000 rows (136.88KB), Cpu time: 466.97us, Blocked wall time: 223.00us, Peak memory: 1.00MB, Threads: 1, CPU breakdown: I/O/F (137.69us/327.61us/1.66us)
            dynamicFiltersProduced    sum: 1, count: 1, min: 1, max: 1
            queuedWallNanos           sum: 24.00us, count: 1, min: 24.00us, max: 24.00us
        -> TableScan[Table: Orders]
           Input: 2000 rows (118.12KB), Raw Input: 20480 rows (72.31KB), Output: 2000 rows (118.12KB), Cpu time: 8.89ms, Blocked wall time: 10.00us, Peak memory: 1.00MB, Threads: 1, Splits: 20, CPU breakdown: I/O/F (0ns/8.88ms/4.93us)
              dataSourceWallNanos       sum: 2.52ms, count: 40, min: 12.00us, max: 250.00us
              dynamicFiltersAccepted    sum: 1, count: 1, min: 1, max: 1
              localReadBytes            sum: 0B, count: 1, min: 0B, max: 0B
              numLocalRead              sum: 0, count: 1, min: 0, max: 0
              numPrefetch               sum: 28, count: 1, min: 28, max: 28
              numRamRead                sum: 0, count: 1, min: 0, max: 0
              numStorageRead            sum: 140, count: 1, min: 140, max: 140
              prefetchBytes             sum: 29.51KB, count: 1, min: 29.51KB, max: 29.51KB
              queuedWallNanos           sum: 29.00us, count: 1, min: 29.00us, max: 29.00us
              ramReadBytes              sum: 0B, count: 1, min: 0B, max: 0B
              skippedSplitBytes         sum: 0B, count: 1, min: 0B, max: 0B
              skippedSplits             sum: 0, count: 1, min: 0, max: 0
              skippedStrides            sum: 0, count: 1, min: 0, max: 0
              storageReadBytes          sum: 150.25KB, count: 1, min: 150.25KB, max: 150.25KB
              totalScanTime             sum: 0ns, count: 1, min: 0ns, max: 0ns
              totalRemainingFilterTime  sum: 0ns, count: 1, min: 0ns, max: 0ns
              queryThreadIoLatency      sum: 0, count: 1, min: 0, max: 0
        -> Project[expressions: (u_c0:INTEGER, ROW["c0"]), (u_c1:BIGINT, ROW["c1"])]
           Output: 100 rows (1.31KB), Cpu time: 43.22us, Blocked wall time: 0ns, Peak memory: 0B, Threads: 1, CPU breakdown: I/O/F (691ns/5.54us/36.98us)
          -> Values[100 rows in 1 vectors]
             Input: 0 rows (0B), Output: 100 rows (1.31KB), Cpu time: 3.05us, Blocked wall time: 0ns, Peak memory: 0B, Threads: 1, CPU breakdown: I/O/F (0ns/2.48us/568ns)

And this is the output for the aggregation query from above.

`printPlanWithStats(*plan, task->taskStats())` shows basic statistics:

.. code-block::

   -> Aggregation[PARTIAL [c5] a0 := max(ROW["c0"]), a1 := sum(ROW["c1"]), a2 := sum(ROW["c2"]), a3 := sum(ROW["c3"]), a4 := sum(ROW["c4"])]
      Output: 849 rows (84.38KB), Cpu time: 1.96ms, Blocked wall time: 0ns, Peak memory: 1.00MB, Threads: 1, CPU breakdown: I/O/F (1.38ms/579.12us/6.82us)
     -> TableScan[Table: hive_table]
        Input: 10000 rows (0B), Output: 10000 rows (0B), Cpu time: 2.89ms, Blocked wall time: 25.00us, Peak memory: 1.00MB, Threads: 1, Splits: 1, CPU breakdown: I/O/F (0ns/2.89ms/3.35us)

`printPlanWithStats(*plan, task->taskStats(), true)` includes custom statistics:

.. code-block::

    -> Aggregation[PARTIAL [c5] a0 := max(ROW["c0"]), a1 := sum(ROW["c1"]), a2 := sum(ROW["c2"]), a3 := sum(ROW["c3"]), a4 := sum(ROW["c4"])]
       Output: 849 rows (84.38KB), Cpu time: 1.96ms, Blocked wall time: 0ns, Peak memory: 1.00MB, Threads: 1, CPU breakdown: I/O/F (1.38ms/579.12us/6.82us)
      -> TableScan[Table: hive_table]
         Input: 10000 rows (0B), Output: 10000 rows (0B), Cpu time: 2.89ms, Blocked wall time: 30.00us, Peak memory: 1.00MB, Threads: 1, Splits: 1, CPU breakdown: I/O/F (0ns/2.89ms/3.35us)
            dataSourceLazyWallNanos    sum: 1.07ms, count: 7, min: 92.00us, max: 232.00us
            dataSourceWallNanos        sum: 329.00us, count: 2, min: 48.00us, max: 281.00us
            loadedToValueHook          sum: 50000, count: 5, min: 10000, max: 10000
            localReadBytes             sum: 0B, count: 1, min: 0B, max: 0B
            numLocalRead               sum: 0, count: 1, min: 0, max: 0
            numPrefetch                sum: 2, count: 1, min: 2, max: 2
            numRamRead                 sum: 0, count: 1, min: 0, max: 0
            numStorageRead             sum: 7, count: 1, min: 7, max: 7
            prefetchBytes              sum: 31.13KB, count: 1, min: 31.13KB, max: 31.13KB
            queuedWallNanos            sum: 101.00us, count: 1, min: 101.00us, max: 101.00us
            ramReadBytes               sum: 0B, count: 1, min: 0B, max: 0B
            skippedSplitBytes          sum: 0B, count: 1, min: 0B, max: 0B
            skippedSplits              sum: 0, count: 1, min: 0, max: 0
            skippedStrides             sum: 0, count: 1, min: 0, max: 0
            storageReadBytes           sum: 61.53KB, count: 1, min: 61.53KB, max: 61.53KB
            totalScanTime              sum: 0ns, count: 1, min: 0ns, max: 0ns
            totalRemainingFilterTime   sum: 0ns, count: 1, min: 0ns, max: 0ns
            queryThreadIoLatency       sum: 0, count: 1, min: 0, max: 0

Common operator statistics
--------------------------

Let’s take a closer look at statistics that are collected for all operators.

For each operator, Velox tracks the total number of input rows, output rows,
their estimated sizes, cpu time, blocked wall time, and the number of threads used to run the operator.

.. code-block::

    -> TableScan[Table: Orders]
           Input: 2000 rows (118.12KB), Raw Input: 20480 rows (72.31KB), Output: 2000 rows (118.12KB), Cpu time: 8.89ms, Blocked wall time: 10.00us, Peak memory: 1.00MB, Threads: 1, Splits: 20, CPU breakdown: I/O/F (0ns/8.88ms/4.93us)


printPlanWithStats shows output rows and
sizes for each plan node and shows input rows and sizes for leaf nodes and nodes
that expand to multiple operators. Showing input rows for other nodes is redundant
since the number of input rows equals the number of output rows of the immediate child plan node.

.. code-block::

	Input: 2000 rows (118.12KB), Output: 2000 rows (118.12KB)

When rows are pruned for a TableScan with filters, Velox reports the number
of raw input rows and their total size. These are the rows processed before
applying the pushed down filters.
TableScan also reports the number of splits assigned.

.. code-block::

	Raw Input: 20480 rows (72.31KB), Splits: 20

Velox also measures CPU time and peak memory usage for each operator. This
information is shown for all plan nodes.

.. code-block::

	Cpu time: 8.89ms, Peak memory: 1.00MB

A breakdown of CPU time into addInput, getOutput and finish stages of the operator is also available. I/O/F below is a shortcut for addInput/getOutput/finish.

.. code-block::

   CPU breakdown: I/O/F (0ns/8.88ms/4.93us)

Some operators like TableScan and HashProbe may be blocked waiting for splits or
hash tables. Velox records the total wall time an operator was blocked and
printPlanWithStats shows this information as “Blocked wall time”.

.. code-block::

	Blocked wall time: 10.00us

Custom operator statistics
--------------------------

Operators also collect and report operator-specific statistics.

TableScan operator reports statistics that show how much data has been read from
cache vs. durable storage, how much data was prefetched, how many files and row
groups were skipped via stats-based pruning.

.. code-block::

   -> TableScan[Table = Orders]
           localReadBytes            sum: 0B, count: 1, min: 0B, max: 0B
           numLocalRead              sum: 0, count: 1, min: 0, max: 0
           numPrefetch               sum: 28, count: 1, min: 28, max: 28
           numRamRead                sum: 0, count: 1, min: 0, max: 0
           numStorageRead            sum: 140, count: 1, min: 140, max: 140
           prefetchBytes             sum: 29.51KB, count: 1, min: 29.51KB, max: 29.51KB
           ramReadBytes              sum: 0B, count: 1, min: 0B, max: 0B
           skippedSplitBytes         sum: 0B, count: 1, min: 0B, max: 0B
           skippedSplits             sum: 0, count: 1, min: 0, max: 0
           skippedStrides            sum: 0, count: 1, min: 0, max: 0
           storageReadBytes          sum: 150.25KB, count: 1, min: 150.25KB, max: 150.25KB
           totalScanTime             sum: 0ns, count: 1, min: 0ns, max: 0ns
           totalRemainingFilterTime  sum: 0ns, count: 1, min: 0ns, max: 0ns
           queryThreadIoLatency      sum: 0, count: 1, min: 0, max: 0

HashBuild operator reports range and number of distinct values for the join keys.

.. code-block::

    -> HashJoin[INNER c0=u_c0]
         HashBuild:
            rangeKey0          sum: 200, count: 1, min: 200, max: 200
            distinctKey0       sum: 101, count: 1, min: 101, max: 101

HashProbe operator reports whether it generated dynamic filter and TableScan
operator reports whether it received dynamic filter pushed down from the join.

.. code-block::

    -> HashJoin[INNER c0=u_c0]
         HashProbe:
            dynamicFiltersProduced    sum: 1, count: 1, min: 1, max: 1
        -> TableScan[]
              dynamicFiltersAccepted     sum: 1, count: 1, min: 1, max: 1

TableScan operator shows how many rows were processed by pushing down aggregation into TableScan.

.. code-block::

    loadedToValueHook          sum: 50000, count: 5, min: 10000, max: 10000
