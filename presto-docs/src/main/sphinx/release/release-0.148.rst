=============
Release 0.148
=============

General Changes
---------------
* Change default ``task.max-worker-threads`` to ``2`` times the number of cores.
* Fix issue where auto-commit transaction can be rolled back for a successfully
  completed query.
* Add ``colocated-joins-enabled`` to enable colocated joins by default for
  connectors that expose node-partitioned data.
* Add support for ``REVOKE`` permission syntax.
* Warn if Presto server is not using G1 garbage collector.

Hive Changes
------------

* Fix ``NoClassDefFoundError`` for ``KMSClientProvider`` in HDFS client.
* Implement ``REVOKE`` permission syntax.
