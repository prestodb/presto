=================================
History based optimizations (HBO)
=================================

HBO is a framework which enables recording of a query's statistics to reuse those statistics for future queries with similar plans. To match queries with similar plans, a query plan
is canonicalized so as to get rid of unrelated differences between plans (for example, naming of intermediate variables in a plan), and each plan node is hashed to a string value.
Historical statistics associated with the same hash value are used in HBO. HBO statistics are preferred over estimations from cost models. The optimizer falls back to cost based
statistics when history statistics are not available.
The statistics recorded for HBO are in `PlanStatistics` structs, which include:

=============================== =============================================================================================================== ===========================================================================================================
Field                           Description                                                                                                     Optimizers
=============================== =============================================================================================================== ===========================================================================================================
rowCount                        Number of rows output by a plan node                                                                            DetermineJoinDistributionType DetermineSemiJoinDistributionType PushPartialAggregationThroughExchange
outputSize                      Size of output by a plan node                                                                                   DetermineJoinDistributionType DetermineSemiJoinDistributionType PushPartialAggregationThroughExchange
joinNodeStatistics              Include total number of input rows and number of rows with NULL join key for both probe and build input         RandomizeNullKeyInOuterJoin
tableWriterNodeStatistics       Number of tasks in table writer                                                                                 ScaledWriterRule
partialAggregationStatistics    Row count and size of input and output of the partial aggregation node                                          PushPartialAggregationThroughExchange
=============================== =============================================================================================================== ===========================================================================================================

How to use HBO
--------------

Presto supports using historical statistics in query optimization. In HBO, statistics of the current query are stored and can be used to optimize future queries.
The Redis HBO Provider can be used as storage for the historical statistics. HBO is controlled by the following session properties:

=========================================================== =========================================================================================================================================================================================================== ===============
Session property                                            Description                                                                                                                                                                                                 Default value
=========================================================== =========================================================================================================================================================================================================== ===============
use_history_based_plan_statistics                           Enable using historical statistics for query optimization                                                                                                                                                   False
track_history_based_plan_statistics                         Enable recording the statistics of the current query as history statistics so as to be used by future queries                                                                                               False
track_history_stats_from_failed_queries                     Track history based plan statistics from complete plan fragments in failed queries                                                                                                                          True
history_based_optimizer_timeout_limit                       Timeout for history based optimizer                                                                                                                                                                         10 seconds
restrict_history_based_optimization_to_complex_query        Enable history based optimization only for complex queries, i.e. queries with join and aggregation                                                                                                          True
history_input_table_statistics_matching_threshold           When the size difference between current table and history table exceed this threshold, do not match history statistics. When value is 0, use the default value set by hbo.history-matching-threshold       0
=========================================================== =========================================================================================================================================================================================================== ===============

Example
-------

An example of a query plan with HBO statistics is shown below. For a plan node, the estimation statistics will show source `HistoryBasedSourceInfo` when the statistics are from HBO.

.. code-block:: text

         Fragment 1 [HASH]                                                                                                                                            >
             Output layout: [orderpriority, count]                                                                                                                    >
             Output partitioning: SINGLE []                                                                                                                           >
             Stage Execution Strategy: UNGROUPED_EXECUTION                                                                                                            >
             - Project[PlanNodeId 392][projectLocality = LOCAL] => [orderpriority:varchar(15), count:bigint]                                                          >
                     Estimates: {source: HistoryBasedSourceInfo, rows: 5 (117B), cpu: ?, memory: ?, network: ?}                                                       >
                 - Aggregate(FINAL)[orderpriority][$hashvalue][PlanNodeId 4] => [orderpriority:varchar(15), $hashvalue:bigint, count:bigint]                          >
                         Estimates: {source: HistoryBasedSourceInfo, rows: 5 (117B), cpu: ?, memory: ?, network: ?}                                                   >
                         count := "presto.default.count"((count_8)) (1:50)                                                                                            >
                     - LocalExchange[PlanNodeId 354][HASH][$hashvalue] (orderpriority) => [orderpriority:varchar(15), count_8:bigint, $hashvalue:bigint]              >
                         - RemoteSource[2] => [orderpriority:varchar(15), count_8:bigint, $hashvalue_9:bigint]

    
Optimizations using HBO
-----------------------

DetermineJoinDistributionType and DetermineSemiJoinDistributionType
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
These two optimizations decide whether to do broadcast or repartition for a join. They use the size of the probe and build input in optimization.

* Data size recorded from history queries will be used when HBO is enabled.
* Statistics from cost models are used when HBO statistics are not available or HBO is disabled.

ReorderJoins
^^^^^^^^^^^^
This optimization reorders the join order based on the size of input and output. Data size recorded from history queries will be used when HBO is enabled.

PushPartialAggregationThroughExchange
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This optimization decides whether to split an aggregation into partial and final aggregations.

* Set `track_partial_aggregation_history` to true to track the output size of the partial aggregation node.
* Set `use_partial_aggregation_history` to true to use the partial aggregation node statistics to decide whether to split aggregation. The track of partial aggregation statistics targets the pattern we found in production queries, where the final aggregation node is cardinality reducing but partial aggregation is not. When `use_partial_aggregation_history` is not enabled or partial aggregation statistics are not available, it will fall back to using the final aggregation statistics.

Note: When the optimizer disables partial aggregation, there are no statistics about partial aggregation and the partial aggregation statistics are unavailable.

ScaledWriterRule
^^^^^^^^^^^^^^^^
Scaled writer supports dynamically increasing the number of file write tasks, so as to avoid writing out too many small files. By default it starts with one write task.
In HBO, the number of tasks used for writing files is recorded as history. ScaledWriterRule decides the number of tasks to start with based on this information.
It will start with half of the number of write tasks recorded in HBO, because scaled writer only increases the number of write tasks and it will never decrease if we use
exactly the same number of tasks from history runs. This optimization can be enabled by session property `enable_history_based_scaled_writer`.

RandomizeNullKeyInOuterJoin
^^^^^^^^^^^^^^^^^^^^^^^^^^^
RandomizeNullKeyInOuterJoin is used to mitigate skew of NULL values in outer joins by rewriting NULL keys to non null keys which will never match.
It benefits queries with outer joins where the join key has skew on NULL values.
In HBO, the number of NULL keys and total join keys are tracked for join nodes; this optimization will be enabled when the portion of NULL keys exceeds the following thresholds:

* The number of NULL keys, which is hardcoded to 100,000.
* The portion of NULL keys, which can be set by session property `randomize_outer_join_null_key_null_ratio_threshold` and defaults to 2%.

This optimization can be enabled by setting `randomize_outer_join_null_key_strategy` to `COST_BASED`.
