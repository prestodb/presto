********************
April 2022 Update
********************

Documentation
=============

* Add :doc:`What’s in the Task? <../develop/task>` documentation article.
* Document `lifecycle of Tasks, Drivers and Operators <https://github.com/facebookincubator/velox/blob/main/velox/exec/TaskDriverOperatorLifecycle.md>`_.
* Document :doc:`printExprWithStats <../develop/debugging/print-expr-with-stats>` debugging tool.

Core Library
============

* Add support for UNNEST for multiple columns.
* Add support for running final group-by aggregations using multiple threads.
* Add `xsimd <https://github.com/xtensor-stack/xsimd>`_ dependency. :pr:`1405`
* Upgrade folly to 2022.03.14.00 (from 2021.05.10.00).
* Upgrade fmt to 8.0.0 (from 7.1.3).
* Optimize MergeJoin operator by removing unnecessary copying of std::shared_ptr.
* Optimize LocalMerge operator. :pr:`1318`
* Fix coalesce Presto function to avoid evaluating arguments that are not needed by replacing it with a build-in special form. :pr:`1430`
* Fix DISTINCT + LIMIT queries. :pr:`1390`

Presto Functions
================

* Re-implement :func:`approx_percentile` aggregate function using KLL sketch instead of T-Digest and add support for accuracy parameter.

Performance and Correctness Testing
===================================

* Add support for TPC-H data generation through dbgen integration. Support for TPC-H connector is coming soon.
* Add q13 to TPC-H benchmark.
* Add AggregationTestBase::testAggregations helper method to test a variety of logically equivalent plans to compute aggregations using combinations of partial, final, single, and intermediate aggregations with and without local exchanges to simplify testing of individual aggregate function with better coverage. :pr:`1476`
* Add support for ORDER BY SQL clauses to PlanBuilder::topN() and PlanBuilder::mergeExchange().
* Add support for specifying range and remaining filters using SQL in PlanBuilder::tableScan().
* Simplify PlanBuilder::xxxAggregation() and PlanBuilder::partitionedOutput() APIs by taking grouping and partitioning keys by name.
* Upgrade GTest submodule to 1.11 (from 1.10).

Debugging Experience
====================

* Add support for collecting runtime statistics for individual expressions.
* Add printExprWithStats function to print expression tree annotated with runtime statistics. :pr:`1425`
* Add number of splits and threads to printPlanWithStats output.
* Add filters pushed down into table scan to printPlanWithStats output.
* Add a mechanism to register a listener that is called upon task completion and receives task completion status, error details and runtime statistics. :pr:`1404`
* Add a mechanism to register a listener that is called on destruction of ExprSet and receives runtime statistics. :pr:`1428`

Credits
=======

Alexey Spiridonov, Andrii Vasylevskyi, Carlos Torres, David Greenberg, Deepak
Majeti, Ge Gao, Huameng Jiang, James Xu, Jialiang Tan, Jimmy Lu, Jon Janzen,
Jun Wu, Katie Mancini, Kevin Wilfong, Krishna Pai, Laith Sakka, Li Yazhou,
MJ Deng, Masha Basmanova, Orri Erling, Pedro Eugenio Rocha Pedreira, Pyre Bot Jr,
Richard Barnes, Sergey Pershin, Victor Zverovich, Wei He, Wenlei Xie, Xiang Xu,
Zeyi (Rice) Fan, qiaoyi.dingqy