=============
Release 0.215
=============

General Changes
---------------

* Add dynamic schedule support for grouped execution . When a stage can be executed
  with grouped execution and the stage has no remote source, table partitions can be
  scheduled to tasks in a dynamic way. This can mitigate skew for grouped executed queries.
  The dynamic schedule feature for grouped execution must be enabled with the
  ``dynamic_schedule_for_grouped_execution`` config property or the
  ``dynamic-schedule-for-grouped-execution`` config property.
* Enable spatial joins for join conditions expressed with :func:`ST_Within`
* Improve spatial join performance by accelerating build-side geometries.
* Add input validation checks to :func:`ST_LineString` to conform with the specification
* Add :func:`beta_cdf` and :func:`inverse_beta_cdf` functions.
* Split reporting of raw input data and processed input data for source operators.
* Correctly report processed input data for source stages in EXPLAIN ANALYZE.
* Don't report raw input data for Values, Local Exchange and Local Merge So
* Simplify EXPLAIN (TYPE IO) output when there are too many discrete components.
* Adding multiple null convention support for polymorphic scalar functions.
* Bugfix: Fail query that uses non-leaf group so that it does not hang forever.
* Improve performance of calculating block size when complex blocks are involved.
* Fix bug in lazy hashtable build for map.
* Extend stats and cost model to support ROW_NUMBER window function estimates.
* Add :func:`parse_presto_data_size` function.
* Improve join type selection and join sides flipping for the cases when the join output cannot be estimated.
* Support UNION ALL in cost model.
* Optionally use default filter factor to estimate filter node.

Hive Changes
------------

* Add predicate pushdown support for the ``DATE`` type to the Parquet reader. This change also fixes
  a bug that may cause queries with predicates on ``DATE`` columns to fail with type mismatch errors.

Web UI Changes
--------------
* Make Cumulative Memory usage reporting consistent on query list and query details pages.
* Fix ``Capture Snapshot`` button for showing current thread stacks.
* Improve performance of thread snapshot component of worker status page.
* Fix drop-down to show stage skew bar chart.

PostgreSQL Connector Changes
-------------------------------

* Upgrade PostgreSQL JDBC driver to 42.2.5 (from 42.1.4).

Redis Connector
-----------------

* Do not print value of ``redis.password`` configuration property to Presto log file.
