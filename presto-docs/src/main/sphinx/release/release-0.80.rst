============
Release 0.80
============

Hive Changes
------------
We have added a new ORC reader implementation. The new reader supports vectorized
reads, lazy loading, and predicate push down, all of which make the reader more
efficient and typically reduces wall clock time for a query. Although the new
reader has been heavily tested, it is an extensive rewrite of the Apache Hive
ORC reader, and may have some latent issues. If you are seeing issues, you can
disable the new reader on a per-query basis setting the
`<hive-catalog>.optimized_reader_enabled` session property, or you can disable
the reader by default using setting the Hive catalog property
`hive.optimized-reader.enabled`.

* The maximum retry time for the Hive S3 file system can be configured
  by setting ``hive.s3.max-retry-time=false``.
* Fix Hive partition pruning for null (i.e., `__HIVE_DEFAULT_PARTITION__`) keys.

Cassandra Changes
-----------------

* Update Cassandra driver to 2.1.0.

General Changes
---------------

* Add support implicit joins. The following syntax is now allowed::

    SELECT * FROM t, u

* Add property ``task.verbose-stats`` to enable verbose statistics collection for
  tasks. The default is ``false``.

* Format binary data in the CLI as a hex dump.

* Implement an optimization that rewrites aggregation queries that are insensitive to the
  cardinality of the input (e.g., :func:`max`, :func:`min`, `DISTINCT` aggregates) to execute
  against table metadata.

  For example, if ``key``, ``key1`` and ``key2`` are partition keys, the following queries
  will benefit::

      SELECT min(key), max(key) FROM t;

      SELECT DISTINCT key FROM t;

      SELECT count(DISTINCT key) FROM t;

      SELECT count(DISTINCT key + 5) FROM t;

      SELECT count(DISTINCT key) FROM (SELECT key FROM t ORDER BY 1 LIMIT 10);

      SELECT key1, count(DISTINCT key2) FROM t GROUP BY 1;

  This optimization is turned off by default. To turn it on, add ``optimizer.optimize-metadata-queries=true``
  to the coordinator config properties.

  .. warning::

        This optimization will cause queries to produce incorrect results if
        the storage backend allows partitions to contain no data.

* Add approximate numeric histogram function :func:`numeric_histogram`
* Add :func:`array_sort` function.
* Add :func:`map_keys` and :func:`map_values` functions.
* Make :func:`row_number` completely streaming.
