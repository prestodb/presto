============
Release 0.80
============

Hive Changes
------------

* The maximum retry time for the Hive S3 file system can be configured
  by setting ``hive.s3.max-retry-time``.

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
