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
The Redis HBO Provider can be used as storage for the historical statistics. HBO is controlled by the following configuration properties and session properties:

Configuration Properties
^^^^^^^^^^^^^^^^^^^^^^^^

The following configuration properties are available for HBO:

============================================================= =========================================================================================================================== ===================================
Configuration Property Name                                   Description                                                                                                                 Default value
============================================================= =========================================================================================================================== ===================================
``optimizer.use-history-based-plan-statistics``               Use historical statistics for query optimization.                                                                           ``False``
``optimizer.track-history-based-plan-statistics``             Recording the statistics of the current query as history statistics so as to be used by future queries.                     ``False``
``optimizer.track-history-stats-from-failed-queries``         Track history based plan statistics from complete plan fragments in failed queries.                                         ``True``
``optimizer.history-based-optimizer-timeout``                 Timeout for history based optimizer.                                                                                        ``10 seconds``
``optimizer.enforce-timeout-for-hbo-query-registration``      Enforce timeout for query registration in HBO optimizer                                                                     ``False``
``optimizer.treat-low-confidence-zero-estimation-as-unknown`` Treat ``LOW`` confidence, zero estimations as ``UNKNOWN`` during joins.                                                     ``False``
``optimizer.confidence-based-broadcast``                      Broadcast based on the confidence of the statistics that are being used, by broadcasting the side of a joinNode which       ``False``
                                                              has the highest confidence statistics. If confidence is the same, then the original behavior will be followed.
``optimizer.retry-query-with-history-based-optimization``     Retry a failed query automatically if HBO can help change the existing query plan                                           ``False``
``hbo.history-matching-threshold``                            When the size difference between current table and history table exceeds this threshold, do not match history statistics.   ``0.1``
                                                              When value is 0.0, only match history statistics when the size of the two are exactly the same.
``hbo.max-last-runs-history``                                 Number of last runs for which historical stats are stored.                                                                  ``10``
============================================================= =========================================================================================================================== ===================================

Session Properties
^^^^^^^^^^^^^^^^^^

Session properties set behavior changes for queries executed within the given session. When setting, they will overwrite the value of corresponding configuration properties (if any) in the current session.

=========================================================== ==================================================================================================== ==============================================================
Session property Name                                       Description                                                                                          Default value
=========================================================== ==================================================================================================== ==============================================================
``use_history_based_plan_statistics``                       Overrides the behavior of the configuration property                                                 ``optimizer.use-history-based-plan-statistics``
                                                            ``optimizer.use-history-based-plan-statistics`` in the current session.
``track_history_based_plan_statistics``                     Overrides the behavior of the configuration property                                                 ``optimizer.track-history-based-plan-statistics``
                                                            ``optimizer.track-history-based-plan-statistics`` in the current session.
``track_history_stats_from_failed_queries``                 Overrides the behavior of the configuration property                                                 ``optimizer.track-history-stats-from-failed-queries``
                                                            ``optimizer.track-history-stats-from-failed-queries`` in the current session.
``history_based_optimizer_timeout_limit``                   Overrides the behavior of the configuration property                                                 ``optimizer.history-based-optimizer-timeout``
                                                            ``optimizer.history-based-optimizer-timeout`` in the current session.
``enforce_history_based_optimizer_register_timeout``        Overrides the behavior of the configuration property                                                 ``optimizer.enforce-timeout-for-hbo-query-registration``
                                                            ``optimizer.enforce-timeout-for-hbo-query-registration`` in the current session.
``restrict_history_based_optimization_to_complex_query``    Enable history based optimization only for complex queries, i.e. queries with join and aggregation.  ``True``
``history_input_table_statistics_matching_threshold``       Overrides the behavior of the configuration property                                                 ``hbo.history-matching-threshold``
                                                            ``hbo.history-matching-threshold`` in the current session.
``treat_low_confidence_zero_estimation_unknown_enabled``    Overrides the behavior of the configuration property
                                                            ``optimizer.treat-low-confidence-zero-estimation-as-unknown`` in the current session.                ``optimizer.treat-low-confidence-zero-estimation-as-unknown``
``confidence_based_broadcast_enabled``                      Overrides the behavior of the configuration property
                                                            ``optimizer.confidence-based-broadcast`` in the current session.                                     ``optimizer.confidence-based-broadcast``
``retry_query_with_history_based_optimization``             Overrides the behavior of the configuration property
                                                            ``optimizer.retry-query-with-history-based-optimization`` in the current session.                    ``optimizer.retry-query-with-history-based-optimization``
=========================================================== ==================================================================================================== ==============================================================

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
