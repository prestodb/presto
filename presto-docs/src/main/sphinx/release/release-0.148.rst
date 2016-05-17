=============
Release 0.148
=============

General Changes
---------------
* Fix issue where auto-commit transaction can be rolled back for a successfully
  completed query.
* Fix detection of colocated joins.
* Fix planning bug involving partitioning with constants.
* Change default ``task.max-worker-threads`` to ``2`` times the number of cores.
* Add ``colocated-joins-enabled`` to enable colocated joins by default for
  connectors that expose node-partitioned data.
* Add support for ``REVOKE`` permission syntax.
* Warn if Presto server is not using G1 garbage collector.
* Improve planning of co-partitioned JOIN and UNION.
* Improve planning of aggregations over partitioned data.
* Add support for colocated unions.
* Use HTTPS in JDBC driver when using port 443.

Hive Changes
------------

* Fix ``NoClassDefFoundError`` for ``KMSClientProvider`` in HDFS client.
* Fix creating tables on S3 in an empty database.
* Implement ``REVOKE`` permission syntax.
