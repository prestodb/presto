=============
Release 0.148
=============

General Changes
---------------
* Fix issue where auto-commit transaction can be rolled back for a successfully
  completed query.
* Fix detection of colocated joins.
* Fix planning bug involving partitioning with constants.
* Fix window functions to correctly handle empty frames between unbounded and
  bounded in the same direction. For example, a frame such as
  ``ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING``
  would incorrectly use the first row as the window frame for the first two
  rows rather than using an empty frame.
* Fix correctness issue when grouping on columns that are also arguments to aggregation functions.
* Fix failure when chaining ``AT TIME ZONE``, e.g.
  ``SELECT TIMESTAMP '2016-01-02 12:34:56' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC'``.
* Fix data duplication when ``task.writer-count`` configuration mismatches between coordinator and worker.
* Fix bug where ``node-scheduler.max-pending-splits-per-node-per-task`` config is not always
  honored by node scheduler. This bug could stop the cluster from making further progress.
* Fix incorrect results for grouping sets with partitioned source.
* Add ``colocated-joins-enabled`` to enable colocated joins by default for
  connectors that expose node-partitioned data.
* Add support for colocated unions.
* Reduce initial memory usage of :func:`array_agg` function.
* Improve planning of co-partitioned ``JOIN`` and ``UNION``.
* Improve planning of aggregations over partitioned data.
* Improve the performance of the :func:`array_sort` function.
* Improve outer join predicate push down.
* Increase default value for ``query.initial-hash-partitions`` to ``100``.
* Change default value of ``query.max-memory-per-node`` to ``10%`` of the Java heap.
* Change default ``task.max-worker-threads`` to ``2`` times the number of cores.
* Use HTTPS in JDBC driver when using port 443.
* Warn if Presto server is not using G1 garbage collector.
* Move interval types out of SPI.

Interval Fixes
--------------

This release fixes several problems with large and negative intervals.

* Fix parsing of negative interval literals. Previously, the sign of each field was treated
  independently instead of applying to the entire interval value. For example, the literal
  ``INTERVAL '-2-3' YEAR TO MONTH`` was interpreted as a negative interval of ``21`` months
  rather than ``27`` months (positive ``3`` months was added to negative ``24`` months).
* Fix handling of ``INTERVAL DAY TO SECOND`` type in REST API. Previously, intervals greater than
  ``2,147,483,647`` milliseconds (about ``24`` days) were returned as the wrong value.
* Fix handling of ``INTERVAL YEAR TO MONTH`` type. Previously, intervals greater than
  ``2,147,483,647`` months were returned as the wrong value from the REST API
  and parsed incorrectly when specified as a literal.
* Fix formatting of negative intervals in REST API. Previously, negative intervals
  had a negative sign before each component and could not be parsed.
* Fix formatting of negative intervals in JDBC ``PrestoInterval`` classes.

.. note::

    Older versions of the JDBC driver will misinterpret most negative
    intervals from new servers. Make sure to update the JDBC driver
    along with the server.

Functions and Language Features
-------------------------------

* Add :func:`element_at` function for map type.
* Add :func:`split_to_map` function.
* Add :func:`zip` function.
* Add :func:`map_union` aggregation function.
* Add ``ROW`` syntax for constructing row types.
* Add support for ``REVOKE`` permission syntax.
* Add support for ``SMALLINT`` and ``TINYINT`` types.
* Add support for non-equi outer joins.

Verifier Changes
----------------

* Add ``skip-cpu-check-regex`` config property which can be used to skip the CPU
  time comparison for queries that match the given regex.
* Add ``check-cpu`` config property which can be used to disable CPU time comparison.

Hive Changes
------------

* Fix ``NoClassDefFoundError`` for ``KMSClientProvider`` in HDFS client.
* Fix creating tables on S3 in an empty database.
* Implement ``REVOKE`` permission syntax.
* Add support for ``SMALLINT`` and ``TINYINT``
* Support ``DELETE`` from unpartitioned tables.
* Add support for Kerberos authentication when talking to Hive/HDFS.
* Push down filters for columns of type ``DECIMAL``.
* Improve CPU efficiency when reading ORC files.

Cassandra Changes
-----------------

* Allow configuring load balancing policy and no host available retry.
* Add support for ``varchar(n)``.

Kafka Changes
-------------

* Update to Kafka client 0.8.2.2. This enables support for LZ4 data.

JMX Changes
-----------

* Add ``jmx.history`` schema with in-memory periodic samples of values from JMX MBeans.

MySQL and PostgreSQL Changes
----------------------------

* Push down predicates for ``VARCHAR``, ``DATE``, ``TIME`` and ``TIMESTAMP`` types.

Other Connector Changes
-----------------------

* Add support for ``varchar(n)`` to the Redis, TPCH, MongoDB, Local File
  and Example HTTP connectors.

