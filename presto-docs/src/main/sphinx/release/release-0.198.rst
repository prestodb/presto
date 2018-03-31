=============
Release 0.198
=============

General Changes
---------------

* Perform semantic analysis before enqueuing queries.
* Add support for selective aggregates (``FILTER``) with ``DISTINCT`` argument qualifiers.
* Support ``ESCAPE`` for ``LIKE`` predicate in ``SHOW SCHEMAS`` and ``SHOW TABLES`` queries.
* Parse decimal literals (e.g. ``42.0``) as ``DECIMAL`` by default. Previously, they were parsed as
  ``DOUBLE``. This behavior can be turned off via the ``parse-decimal-literals-as-double`` config option or
  the ``parse_decimal_literals_as_double`` session property.
* Fix ``current_date`` failure when the session time zone has a "gap" at ``1970-01-01 00:00:00``.
  The time zone ``America/Bahia_Banderas`` is one such example.
* Add variant of :func:`sequence` function for ``DATE`` with an implicit one-day step increment.
* Increase the maximum number of arguments for the :func:`zip` function from 4 to 5.
* Add :func:`ST_IsValid`, :func:`geometry_invalid_reason`, :func:`simplify_geometry`, and
  :func:`great_circle_distance` functions.
* Support :func:`min` and :func:`max` aggregation functions when the input type is unknown at query analysis time.
  In particular, this allows using the functions with ``NULL`` literals.
* Add configuration property ``task.max-local-exchange-buffer-size`` for setting local exchange buffer size.
* Add trace token support to the scheduler and exchange HTTP clients. Each HTTP request sent
  by the scheduler and exchange HTTP clients will have a "trace token" (a unique ID) in their
  headers, which will be logged in the HTTP request logs. This information can be used to
  correlate the requests and responses during debugging.
* Improve query performance when dynamic writer scaling is enabled.
* Improve performance of :func:`ST_Intersects`.
* Improve query latency when tables are known to be empty during query planning.
* Optimize :func:`array_agg` to avoid excessive object overhead and native memory usage with G1 GC.
* Improve performance for high-cardinality aggregations with ``DISTINCT`` argument qualifiers. This
  is an experimental optimization that can be activated by disabling the `use_mark_distinct` session
  property or the ``optimizer.use-mark-distinct`` config option.
* Improve parallelism of queries that have an empty grouping set.
* Improve performance of join queries involving the :func:`ST_Distance` function.

Resource Groups Changes
-----------------------

* Query Queues have been removed. Resource Groups are always enabled. The
  config property ``experimental.resource-groups-enabled`` has been removed.
* Change ``WEIGHTED_FAIR`` scheduling policy to select oldest eligible sub group
  of groups where utilization and share are identical.

CLI Changes
-----------

* The ``--enable-authentication`` option has been removed. Kerberos authentication
  is automatically enabled when ``--krb5-remote-service-name`` is specified.
* Kerberos authentication now requires HTTPS.

Hive Changes
------------

* Add support for using `AWS Glue <https://aws.amazon.com/glue/>`_ as the metastore.
  Enable it by setting the ``hive.metastore`` config property to ``glue``.
* Fix a bug in the ORC writer that will write incorrect data of type ``VARCHAR`` or ``VARBINARY``
  into files.

JMX Changes
-----------

* Add wildcard character ``*`` which allows querying several MBeans with a single query.

SPI Changes
-----------

* Add performance statistics to query plan in ``QueryCompletedEvent``.
* Remove ``Page.getBlocks()``. This call was rarely used and performed an expensive copy.
  Instead, use ``Page.getBlock(channel)`` or the new helper ``Page.appendColumn()``.
* Improve validation of ``ArrayBlock``, ``MapBlock``, and ``RowBlock`` during construction.
