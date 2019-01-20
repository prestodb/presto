=============
Release 0.215
=============

General Changes
---------------

* Fix regression in 0.214 that could cause queries to produce incorrect results for queries
  using map types.
* Fix reporting of the processed input data for source stages in ``EXPLAIN ANALYZE``.
* Fail queries that use non-leaf resource groups. Previously, they would remain queued forever.
* Improve CPU usage for specific queries (:issue:`x11757`).
* Extend stats and cost model to support :func:`row_number` window function estimates.
* Improve the join type selection and the reordering of join sides for cases where
  the join output size cannot be estimated.
* Add dynamic scheduling support to grouped execution. When a stage is executed
  with grouped execution and the stage has no remote sources, table partitions can be
  scheduled to tasks in a dynamic way, which can help mitigating skew for queries using
  grouped execution. This feature can be enabled with the
  ``dynamic_schedule_for_grouped_execution`` session property or the
  ``dynamic-schedule-for-grouped-execution`` config property.
* Add :func:`beta_cdf` and :func:`inverse_beta_cdf` functions.
* Split the reporting of raw input data and processed input data for source operators.
* Remove collection and reporting of raw input data statistics for the ``Values``,
  ``Local Exchange``, and ``Local Merge Sort`` operators.
* Simplify ``EXPLAIN (TYPE IO)`` output when there are too many discrete components.
  This avoids large output at the cost of reduced granularity.
* Add :func:`parse_presto_data_size` function.
* Add support for ``UNION ALL`` to optimizer's cost model.
* Add support for estimating the cost of filters by using a default filter factor.
  The default value for the filter factor can be configured with the ``default_filter_factor_enabled``
  session property or the ``optimizer.default-filter-factor-enabled``.

Geospatial Changes
------------------

* Add input validation checks to :func:`ST_LineString` to conform with the specification.
* Improve spatial join performance.
* Enable spatial joins for join conditions expressed with the :func:`ST_Within` function.

Web UI Changes
--------------

* Fix *Capture Snapshot* button for showing current thread stacks.
* Fix dropdown for expanding stage skew component on the query details page.
* Improve the performance of the thread snapshot component on the worker status page.
* Make the reporting of *Cumulative Memory* usage consistent on the query list and query details pages.
* Remove legacy thread UI.

Hive Changes
------------

* Add predicate pushdown support for the ``DATE`` type to the Parquet reader. This change also fixes
  a bug that may cause queries with predicates on ``DATE`` columns to fail with type mismatch errors.

Redis Changes
-------------

* Prevent printing the value of the ``redis.password`` configuration property to log files.
