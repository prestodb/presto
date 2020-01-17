=============
Release 0.170
=============

General Changes
---------------

* Fix race condition that could cause queries to fail with ``InterruptedException`` in rare cases.
* Fix a performance regression for ``GROUP BY`` queries over ``UNION``.
* Fix a performance regression that occurs when a significant number of exchange
  sources produce no data during an exchange (e.g., in a skewed hash join).

Web UI Changes
--------------

* Fix broken rendering when catalog properties are set.
* Fix rendering of live plan when query is queued.

JDBC Driver Changes
-------------------

* Add support for ``DatabaseMetaData.getTypeInfo()``.

Hive Changes
------------

* Improve decimal support for the Parquet reader.
* Remove misleading "HDFS" string from error messages.

Cassandra Changes
-----------------

* Fix an intermittent connection issue for Cassandra 2.1.
* Remove support for selecting by partition key when the partition key is only partially specified.
  The ``cassandra.limit-for-partition-key-select`` and ``cassandra.fetch-size-for-partition-key-select``
  config options are no longer supported.
* Remove partition key cache to improve consistency and reduce load on the Cassandra cluster due to background cache refresh.
* Reduce the number of connections opened to the Cassandra cluster. Now Presto opens a single connection from each node.
* Use exponential backoff for retries when Cassandra hosts are down. The retry timeout can be controlled via the
  ``cassandra.no-host-available-retry-timeout`` config option, which has a default value of ``1m``.
  The ``cassandra.no-host-available-retry-count`` config option is no longer supported.

Verifier Changes
----------------

* Add support for ``INSERT`` queries.
