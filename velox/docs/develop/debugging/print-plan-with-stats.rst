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

By default, printPlanWithStats shows a number of output rows, CPU time and peak
memory usage for each plan node.

`printPlanWithStats(*plan, task->taskStats())`

.. code-block::

     -> Project[expressions: (c0:INTEGER, ROW["c0"]), (p1:BIGINT, plus(ROW["c1"],1)), (p2:BIGINT, plus(ROW["c1"],ROW["u_c1"])), ]
       Output: 2000 rows (104120 bytes), Cpu time: 3282171ns, Blocked wall time: 0ns, Peak memory: 1048576 bytes
      -> HashJoin[INNER c0=u_c0]
         Output: 2000 rows (85580 bytes), Cpu time: 3488512ns, Blocked wall time: 484000ns, Peak memory: 2097152 bytes
         HashBuild: Output: 0 rows (0 bytes), Cpu time: 420363ns, Blocked wall time: 0ns, Peak memory: 1048576 bytes
         HashProbe: Output: 2000 rows (85580 bytes), Cpu time: 3068149ns, Blocked wall time: 484000ns, Peak memory: 1048576 bytes
        -> TableScan[]
           Input: 11240 rows (60480 bytes), Raw Input: 20480 rows (74041 bytes), Output: 11240 rows (60480 bytes), Cpu time: 16091681ns, Blocked wall time: 14000ns, Peak memory: 1048576 bytes
        -> Project[expressions: (u_c0:INTEGER, ROW["c0"]), (u_c1:BIGINT, ROW["c1"]), ]
           Output: 100 rows (1343 bytes), Cpu time: 64738ns, Blocked wall time: 0ns, Peak memory: 0 bytes
          -> Values[100 rows in 1 vectors]
             Input: 0 rows (0 bytes), Output: 100 rows (1343 bytes), Cpu time: 9793ns, Blocked wall time: 0ns, Peak memory: 0 bytes

With includeCustomStats flag enabled, printPlanWithStats adds operator-specific
statistics for each plan node, e.g. number of distinct values for the join key,
number of row groups skipped in table scan, amount of data read from cache and
storage in table scan, number of rows processed via aggregation pushdown into
scan, etc.

Here is the output for the join query from above.

`printPlanWithStats(*plan, task->taskStats(), true)` shows custom operator statistics.

.. code-block::

    -> Project[expressions: (c0:INTEGER, ROW["c0"]), (p1:BIGINT, plus(ROW["c1"],1)), (p2:BIGINT, plus(ROW["c1"],ROW["u_c1"])), ]
       Output: 2000 rows (104120 bytes), Cpu time: 3282171ns, Blocked wall time: 0ns, Peak memory: 1048576 bytes
          dataSourceLazyWallNanos    sum: 931000, count: 20, min: 36000, max: 102000
      -> HashJoin[INNER c0=u_c0]
         Output: 2000 rows (85580 bytes), Cpu time: 3488512ns, Blocked wall time: 484000ns, Peak memory: 2097152 bytes
         HashBuild: Output: 0 rows (0 bytes), Cpu time: 420363ns, Blocked wall time: 0ns, Peak memory: 1048576 bytes
            queuedWallNanos    sum: 90000, count: 1, min: 90000, max: 90000
            rangeKey0          sum: 200, count: 1, min: 200, max: 200
            distinctKey0       sum: 101, count: 1, min: 101, max: 101
         HashProbe: Output: 2000 rows (85580 bytes), Cpu time: 3068149ns, Blocked wall time: 484000ns, Peak memory: 1048576 bytes
            queuedWallNanos           sum: 36000, count: 1, min: 36000, max: 36000
            dynamicFiltersProduced    sum: 1, count: 1, min: 1, max: 1
        -> TableScan[]
           Input: 11240 rows (60480 bytes), Raw Input: 20480 rows (74041 bytes), Output: 11240 rows (60480 bytes), Cpu time: 16091681ns, Blocked wall time: 14000ns, Peak memory: 1048576 bytes
              queuedWallNanos            sum: 56000, count: 1, min: 56000, max: 56000
              ramReadBytes               sum: 0, count: 1, min: 0, max: 0
              dataSourceLazyWallNanos    sum: 1156000, count: 10, min: 68000, max: 292000
              skippedStrides             sum: 0, count: 1, min: 0, max: 0
              dynamicFiltersAccepted     sum: 1, count: 1, min: 1, max: 1
              localReadBytes             sum: 0, count: 1, min: 0, max: 0
              numRamRead                 sum: 0, count: 1, min: 0, max: 0
              numPrefetch                sum: 38, count: 1, min: 38, max: 38
              prefetchBytes              sum: 62939, count: 1, min: 62939, max: 62939
              skippedSplits              sum: 0, count: 1, min: 0, max: 0
              storageReadBytes           sum: 153855, count: 1, min: 153855, max: 153855
              dataSourceWallNanos        sum: 6341000, count: 40, min: 37000, max: 1282000
              numStorageRead             sum: 140, count: 1, min: 140, max: 140
              skippedSplitBytes          sum: 0, count: 1, min: 0, max: 0
              numLocalRead               sum: 0, count: 1, min: 0, max: 0
        -> Project[expressions: (u_c0:INTEGER, ROW["c0"]), (u_c1:BIGINT, ROW["c1"]), ]
           Output: 100 rows (1343 bytes), Cpu time: 64738ns, Blocked wall time: 0ns, Peak memory: 0 bytes
          -> Values[100 rows in 1 vectors]
             Input: 0 rows (0 bytes), Output: 100 rows (1343 bytes), Cpu time: 9793ns, Blocked wall time: 0ns, Peak memory: 0 bytes

And this is the output for the aggregation query from above.

`printPlanWithStats(*plan, task->taskStats())` shows basic statistics:

.. code-block::

    -> Aggregation[PARTIAL [c5] a0 := max(ROW["c0"]), a1 := sum(ROW["c1"]), a2 := sum(ROW["c2"]), a3 := sum(ROW["c3"]), a4 := sum(ROW["c4"])]
       Output: 849 rows (86400 bytes), Cpu time: 6831560ns, Blocked wall time: 0ns, Peak memory: 1048576 bytes
      -> TableScan[]
         Input: 10000 rows (0 bytes), Output: 10000 rows (0 bytes), Cpu time: 3022092ns, Blocked wall time: 57000ns, Peak memory: 1048576 bytes

`printPlanWithStats(*plan, task->taskStats(), true)` includes custom statistics:

.. code-block::

    -> Aggregation[PARTIAL [c5] a0 := max(ROW["c0"]), a1 := sum(ROW["c1"]), a2 := sum(ROW["c2"]), a3 := sum(ROW["c3"]), a4 := sum(ROW["c4"])]
       Output: 849 rows (86400 bytes), Cpu time: 6831560ns, Blocked wall time: 0ns, Peak memory: 1048576 bytes
      -> TableScan[]
         Input: 10000 rows (0 bytes), Output: 10000 rows (0 bytes), Cpu time: 3022092ns, Blocked wall time: 57000ns, Peak memory: 1048576 bytes
            queuedWallNanos            sum: 233000, count: 1, min: 233000, max: 233000
            ramReadBytes               sum: 0, count: 1, min: 0, max: 0
            dataSourceLazyWallNanos    sum: 2673000, count: 7, min: 165000, max: 709000
            skippedStrides             sum: 0, count: 1, min: 0, max: 0
            loadedToValueHook          sum: 50000, count: 5, min: 10000, max: 10000
            numRamRead                 sum: 0, count: 1, min: 0, max: 0
            numPrefetch                sum: 2, count: 1, min: 2, max: 2
            prefetchBytes              sum: 31880, count: 1, min: 31880, max: 31880
            localReadBytes             sum: 0, count: 1, min: 0, max: 0
            skippedSplits              sum: 0, count: 1, min: 0, max: 0
            storageReadBytes           sum: 63010, count: 1, min: 63010, max: 63010
            dataSourceWallNanos        sum: 2092000, count: 2, min: 103000, max: 1989000
            numStorageRead             sum: 7, count: 1, min: 7, max: 7
            skippedSplitBytes          sum: 0, count: 1, min: 0, max: 0
            numLocalRead               sum: 0, count: 1, min: 0, max: 0

Common operator statistics
--------------------------

Let’s take a closer look at statistics that are collected for all operators.

.. code-block::

    -> TableScan[]
           Input: 11240 rows (60480 bytes), Raw Input: 20480 rows (74041 bytes), Output: 11240 rows (60480 bytes), Cpu time: 16091681ns, Blocked wall time: 14000ns, Peak memory: 1048576 bytes

For each operator, Velox tracks the total number of input and output rows as
well as their estimated size in bytes. printPlanWithStats shows output rows and
bytes for each plan node and shows input rows and bytes for leaf nodes. Showing
input rows for non-leaf nodes is redundant since the number of input rows
equals the number of output rows of the immediate child plan node.

.. code-block::

	Input: 11240 rows (60480 bytes), Output: 11240 rows (60480 bytes)

For TableScan, Velox also reports raw input rows and bytes. These are the rows
processed before applying a pushed down filter.

.. code-block::

	Raw Input: 20480 rows (74041 bytes)

Velox also measures CPU time and peak memory usage for each operator. This
information is shown for all plan nodes.

.. code-block::

	Cpu time: 16091681ns, Peak memory: 1048576 bytes

Some operators like TableScan and HashProbe may be blocked waiting for splits or
hash tables. Velox records the total wall time an operator was blocked and
printPlanWithStats shows this information as “Blocked wall time”.

.. code-block::

	Blocked wall time: 14000ns

Custom operator statistics
--------------------------

Operators also collect and report operator-specific statistics.

TableScan operator reports statistics that show how much data has been read from
cache vs. durable storage, how much data was prefetched, how many files and row
groups were skipped via stats-based pruning.

.. code-block::

    -> TableScan[]
            ramReadBytes               sum: 0, count: 1, min: 0, max: 0
            skippedStrides             sum: 0, count: 1, min: 0, max: 0
            numRamRead                 sum: 0, count: 1, min: 0, max: 0
            numPrefetch                sum: 2, count: 1, min: 2, max: 2
            prefetchBytes              sum: 31880, count: 1, min: 31880, max: 31880
            localReadBytes             sum: 0, count: 1, min: 0, max: 0
            skippedSplits              sum: 0, count: 1, min: 0, max: 0
            storageReadBytes           sum: 63010, count: 1, min: 63010, max: 63010
            numStorageRead             sum: 7, count: 1, min: 7, max: 7
            skippedSplitBytes          sum: 0, count: 1, min: 0, max: 0
            numLocalRead               sum: 0, count: 1, min: 0, max: 0

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

