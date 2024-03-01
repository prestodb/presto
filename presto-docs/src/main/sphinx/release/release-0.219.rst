=============
Release 0.219
=============

General Changes
---------------

* Fix a correctness bug for queries with a ``LIMIT`` on top of a ``FULL OUTER JOIN``.
* Fix a correctness bug in how word boundaries are handled for regular expression functions when using the Joni regex engine.
* Fix query failures caused by Parquet predicate pushdown for ``SMALLINT`` and ``TINYINT`` types.
* Fix query failures when view columns were fully qualified with catalog and/or schema.
* Fix query failures when creating and reading an unpartitioned Hive table within the same transaction.
* Fix an issue that would disable a performance optimization if two tables with mismatching
  but compatible bucket counts are joined and one of the tables has a ``$bucket`` filter.
* Fix an issue that may cause procedure calls to fail if no queries were run after the server started up.
* Fix an issue where the properties supported by the ``ANALYZE`` statement for a given connector would remain in the ``system.metadata.analyze_properties`` table
  even after the connector was removed.
* Add :func:`ST_Length` for ``SphericalGeography`` type..
* Add ``view_owner`` column to the ``information_schema.views`` system table.
* Add a ``JSON`` version of the query plan to ``QueryCompletedEvent``.
* Add support for creating warnings during parsing.
* Add a warning for using the ``current_role`` reserved word as an identifier.
* Add support for using the empty string as a delimiter for the :func:`split` function.
  When an empty string is used as the delimiter, the string will be split into individual characters.

Raptor Changes
--------------
* Add ``raptor.minimum-node-count`` configuration property. If the number of nodes in a cluster is less than the configured value,
  data recovery and reassignment will not run.
* Add ``raptor.startup-grace-period`` configuration property to specify the delay for the initial bucket balancer run after the coordinator startup.

Hive Connector Changes
----------------------

* Add ``hive.ignore-table-bucketing`` configuration property and ``ignore_table_bucketing`` session property.
  When set to true, these properties enable reading from partitions whose bucketing scheme does not match the table bucketing scheme.

Verifier Changes
----------------
* Fix an issue where the determinism check for ``INSERT`` queries were not run when there was a result mismatch.
* Fix an issue where the checksum query was treated as the main query, and the main query was treated as part of the setup queries
  for ``INSERT`` query verification.
* Fix query timeout enforcement by replacing local timer with the ``query_max_execution_time`` session property.
* Improve result comparison for floating point columns by using relative errors.
  Replace configuration property ``expected-double-precision`` with ``relative-error-margin``.
* Improve result comparison for orderable array columns by applying :func:`array_sort` before :func:`checksum`.
* Improve result comparison for ``SELECT`` queries by rewriting ``SELECT`` queries as ``CREATE TABLE AS`` and using checksum queries to verify the results.
  This eliminates the row count limit for ``SELECT`` queries.
* Reuse initial control query results for the determinism check. This reduces the maximum number of control query runs and eliminates the test query reruns.
* Add support for retrying transient query failures using configuration properties ``presto.max-attempts``, ``presto.min-backoff-delay``,
  ``presto.max-backoff-delay``, ``presto.backoff-scale-factor``. Similar configurations prefixed with ``cluster-connection`` instead of ``presto``
  are introduced for retrying transient network failures when communicating with the coordinator. Intermediate failures are recorded and emitted in the output.
* Add support for automatically resolving certain kinds of failures including exceeding the global memory limit and exceeding time limit.
* Add configuration properties ``metadata-timeout`` and ``checksum-timeout`` to set the timeouts for metadata queries
  (i.e., describe queries that read table schema) and checksum queries.
* Add ``source-query.table-name`` configuration property to specify the name of the MySQL table from which the verifier queries will loaded.
* Add configuration property ``human-readable.log-file`` to allow human-readable verification results to be logged into the specified file instead of ``stdout``.
* Rename configuration properties ``query-database`` to ``source-query.database``, ``suites`` to ``source-query.suites``,
  ``max_queries`` to ``source-query.max-queries-per-suite``, ``event-client`` to ``event-clients``, ``event-log-file`` to ``json.log-file``,
  ``run-id`` to ``test-id``, ``thread-count`` to ``max-concurrency``, ``control-gateway`` to ``control.jdbc_url``, ``test-gateway`` to ``test.jdbc_url``,
  ``shadow-writes.control-table-prefix`` to ``control.table-prefix``, and ``shadow-writes.test-table-prefix`` to ``test.table-prefix``.
* Remove configuration properties ``control.query-types``, ``test.query-types``, ``source``, ``max-row-count``, ``always-report``,
  ``skip-correctness-regex``, ``check-correctness``, ``skip-cpu-check-regex``, ``check-cpu``, ``explain-only``, ``verbose-results-comparison``,
  ``quiet``, ``control-teardown-retries``, ``test-teardown-retries``, and ``shadow-writes``.

SPI Changes
-----------
* Expose ``RowExpression`` to SPI, to allow passing resolved expressions to connectors.
