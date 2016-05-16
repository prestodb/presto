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
* Change default ``task.max-worker-threads`` to ``2`` times the number of cores.
* Add ``colocated-joins-enabled`` to enable colocated joins by default for
  connectors that expose node-partitioned data.
* Add support for ``REVOKE`` permission syntax.
* Warn if Presto server is not using G1 garbage collector.
* Improve planning of co-partitioned JOIN and UNION.
* Improve planning of aggregations over partitioned data.
* Add support for colocated unions.
* Use HTTPS in JDBC driver when using port 443.
* Increase default value for ``query.initial-hash-partitions`` to ``100``.
* Add :func:`element_at` function for map type.
* Add :func:`split_to_map` function.
* Add ``ROW`` syntax for constructing row types.
* Reduce initial memory usage of :func:`array_agg` function.
* Change default value of ``query.max-memory-per-node`` to ``10%`` of the Java heap.

Hive Changes
------------

* Fix ``NoClassDefFoundError`` for ``KMSClientProvider`` in HDFS client.
* Fix creating tables on S3 in an empty database.
* Implement ``REVOKE`` permission syntax.


Cassandra Changes
-----------------

* Allow configuring load balancing policy and no host available retry.

Kafka Changes
-------------

* Update to Kafka client 0.8.2.2. This enables support for LZ4 data.

