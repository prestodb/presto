==================
Runtime Statistics
==================

``RuntimeStats`` provides a lightweight way to add diagnostic metrics without
adding fields throughout the query statistics model. A ``RuntimeStats`` object
contains named ``RuntimeMetric`` objects. Each metric tracks the number, sum,
minimum, and maximum of the values recorded for it. Presto merges metrics from
operators and tasks into the query statistics, so metrics with the same name
and unit are aggregated automatically.

Runtime statistics are intended for query diagnostics and performance
investigations. Use the existing JMX metrics infrastructure instead when a
metric describes the server or must be monitored across queries.

Defining a metric
-----------------

Metrics use ``long`` values and one of the following ``RuntimeUnit`` values:

* ``NONE`` for counts and other unitless values
* ``NANO`` for time in nanoseconds
* ``BYTE`` for data sizes in bytes

Use one unit consistently for a metric name. Metrics with the same name are
merged, and merging metrics that have different units fails.

For metrics in Presto core, define a descriptive constant in
``com.facebook.presto.common.RuntimeMetricName`` and use the constant at every
recording site. Include the unit in the metric name when it makes the meaning
clear, for example ``GET_TABLE_TIME_NANOS``. Connectors may define their own
metric names without adding them to ``RuntimeMetricName``. Connector names
should still be stable and descriptive.

Avoid values such as query IDs, table names, partition names, or error messages
in metric names. Every distinct name creates another entry in the query
statistics and prevents values from being aggregated.

Recording values
----------------

Obtain the ``RuntimeStats`` instance associated with the code being measured
and call ``addMetricValue``. Each call records one sample. For example, a
counter records ``1`` for each event:

.. code-block:: java

    runtimeStats.addMetricValue(CACHE_HIT_COUNT, NONE, 1);

A measurement records its value and unit:

.. code-block:: java

    runtimeStats.addMetricValue(INPUT_DATA_BYTES, BYTE, inputDataSizeInBytes);

Use ``addMetricValueIgnoreZero`` only when a zero-valued sample should be
omitted entirely. Omitting it changes both the metric count and its minimum.

``recordWallTime`` measures a ``Runnable`` or ``Supplier`` and records elapsed
wall time with the ``NANO`` unit:

.. code-block:: java

    TableHandle tableHandle = session.getRuntimeStats().recordWallTime(
            GET_TABLE_HANDLE_TIME_NANOS,
            () -> metadata.getTableHandle(session, tableName));

``recordWallAndCpuTime`` also records current-thread CPU time. It uses the
provided name for wall time and appends ``OnCpu`` for the CPU-time metric. Add
and test both names when using this helper.

Selecting the RuntimeStats instance
-----------------------------------

Record a metric in the narrowest scope that owns the measurement. Do not create
an unrelated ``RuntimeStats`` object, because Presto will not know to include it
in the query results. Common sources are:

* ``Session.getRuntimeStats()`` for coordinator-side, query-scoped work.
* ``OperatorContext.getRuntimeStats()`` for work performed by an operator.
* ``ConnectorSession.getRuntimeStats()`` for query-scoped connector work.
* The ``RuntimeStats`` argument to
  ``ConnectorPageSourceProvider.createPageSource`` for page-source work. A page
  source that keeps separate statistics can expose them from
  ``ConnectorPageSource.getRuntimeStats()`` so the engine can collect them.

``RuntimeStats`` and ``RuntimeMetric`` support concurrent updates. Reuse the
instance supplied by the owning scope rather than synchronizing around each
update.

Testing a metric
----------------

Add a focused test at the level where the metric is recorded. After exercising
the operation, get the metric by name and verify its unit and aggregation. In
particular, verify ``count``, ``sum``, ``min``, and ``max`` when the operation
can record multiple samples. Use deterministic values for count and byte
metrics. For timers, verify that the metric is present with the ``NANO`` unit
instead of depending on an exact duration.

Viewing runtime statistics
--------------------------

Runtime statistics are available while a query is running and after it
finishes:

* Open the query details page in the coordinator web interface and view the
  **Runtime Stats** table.
* Run the Presto CLI with both ``--debug`` and ``--runtime-stats``.
* Read ``QueryStatistics.getRuntimeStats()`` from a query completion event in
  an :doc:`event listener <event-listener>`.

The displayed ``sum``, ``count``, ``min``, and ``max`` values reflect all
samples merged under that metric name at the displayed scope. Query-level
metrics originating in a stage have a stage prefix so measurements from
different stages remain distinguishable.
