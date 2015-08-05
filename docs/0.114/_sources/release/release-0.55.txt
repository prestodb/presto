============
Release 0.55
============

RC Binary 2-4x Gain in CPU Efficiency
-------------------------------------

Presto uses custom fast-path decoding logic for specific Hive file
formats.  In this release we have added a fast path for for RCFile when using
the Binary SerDe (``LazyBinaryColumnarSerDe``).  In our
micro benchmarks, we see a gain between 2x and 4x in CPU efficiency compared
to the generic (slow) path.  Since Hive data decoding accounts for a
significant portion of the CPU time, this should
result in measurable gains for most queries over RC Binary encoded data.
Note that this optimization may not result in a reduction in latency
if your cluster is network or disk I/O bound.

Hash Distributed Aggregations
-----------------------------

``GROUP BY`` aggregations are now distributed across a fixed number of machines.
This is controlled by the property ``query.initial-hash-partitions``  set in
``etc/config.properties`` of the coordinator and workers. If the value is
larger than the number of machines available during query scheduling, Presto
will use all available machines.  The default value is ``8``.

The maximum memory size of an aggregation is now
``query.initial-hash-partitions`` times ``task.max-memory``.

Simple Distinct Aggregations
----------------------------

We have added support for the ``DISTINCT`` argument qualifier for aggregation
functions. This is currently limited to queries without a ``GROUP BY`` clause and
where all the aggregation functions have the same input expression. For example::

    SELECT count(DISTINCT country)
    FROM users

Support for complete ``DISTINCT`` functionality is in our roadmap.

Range Predicate Pushdown
------------------------

We've modified the connector API to support range predicates in addition to simple equality predicates.
This lays the ground work for adding connectors to systems that support range
scans (e.g., HBase, Cassandra, JDBC, etc).

In addition to receiving range predicates, the connector can also communicate
back the ranges of each partition for use in the query optimizer.  This can be a
major performance gain for ``JOIN`` queries where one side of the join has
only a few partitions.  For example::

   SELECT * FROM data_1_year JOIN data_1_week USING (ds)

If ``data_1_year`` and ``data_1_week`` are both partitioned on ``ds``, the
connector will report back that one table has partitions for 365 days and the
other table has partitions for only 7 days.  Then the optimizer will limit
the scan of the ``data_1_year`` table to only the 7 days that could possible
match.  These constraints are combined with other predicates in the
query to further limit the data scanned.

.. note::
    This is a backwards incompatible change with the previous connector SPI,
    so if you have written a connector, you will need to update your code
    before deploying this release.

json_array_get Function
-----------------------

The :func:`json_array_get` function makes it simple to fetch a single element from a
scalar json array.

Non-reserved Keywords
---------------------

The keywords ``DATE``, ``TIME``, ``TIMESTAMP``, and ``INTERVAL`` are no longer
reserved keywords in the grammar.  This means that you can access a column
named ``date`` without quoting the identifier.

CLI source Option
-----------------

The Presto CLI now has an option to set the query source.  The source
value is shown in the UI and is recorded in events.   When using the CLI in
shell scripts it is useful to set the ``--source`` option to distinguish shell
scripts from normal users.

SHOW SCHEMAS FROM
-----------------

Although the documentation included the syntax ``SHOW SCHEMAS [FROM catalog]``,
it was not implemented.  This release now implements this statement correctly.

Hive Bucketed Table Fixes
-------------------------

For queries over Hive bucketed tables, Presto will attempt to limit scans to
the buckets that could possible contain rows that match the WHERE clause.
Unfortunately, the algorithm we were using to select the buckets was not
correct, and sometimes we would either select the wrong files or fail to
select any files.  We have aligned
the algorithm with Hive and now the optimization works as expected.

We have also improved the algorithm for detecting tables that are not properly
bucketed.  It is common for tables to declare bucketing in the Hive metadata, but
not actually be bucketed in HDFS.  When Presto detects this case, it fallback to a full scan of the
partition.  Not only does this change make bucketing safer, but it makes it easier
to migrate a table to use bucketing without rewriting all of the data.
