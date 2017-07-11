=============
Release 0.167
=============

General Changes
---------------

* Fix planning failure when a window function depends on the output of another window function.
* Fix planning failure for certain aggregation with both ``DISTINCT`` and ``GROUP BY``.
* Fix incorrect aggregation of operator summary statistics.
* Fix a join issue that could cause joins that produce and filter many rows
  to monopolize worker threads, even after the query has finished.
* Expand plan predicate pushdown capabilities involving implicitly coerced types.
* Short-circuit inner and right join when right side is empty.
* Optimize constant patterns for ``LIKE`` predicates that use an escape character.
* Validate escape sequences in ``LIKE`` predicates per the SQL standard.
* Reduce memory usage of :func:`min_by` and :func:`max_by`.
* Add :func:`transform_keys`, :func:`transform_values` and :func:`zip_with` lambda functions.
* Add :func:`levenshtein_distance` function.
* Add JMX stat for the elapsed time of the longest currently active split.
* Add JMX stats for compiler caches.
* Raise required Java version to 8u92.

Security Changes
----------------

* The ``http.server.authentication.enabled`` config option that previously enabled
  Kerberos has been replaced with ``http-server.authentication.type=KERBEROS``.
* Add support for :doc:`/security/ldap` using username and password.
* Add a read-only :doc:`/develop/system-access-control` named ``read-only``.
* Allow access controls to filter the results of listing catalogs, schemas and tables.
* Add access control checks for :doc:`/sql/show-schemas` and :doc:`/sql/show-tables`.

Web UI Changes
--------------

* Add operator-level performance analysis.
* Improve visibility of blocked and reserved query states.
* Lots of minor improvements.

JDBC Driver Changes
-------------------

* Allow escaping in ``DatabaseMetaData`` patterns.

Hive Changes
------------

* Fix write operations for ``ViewFileSystem`` by using a relative location.
* Remove support for the ``hive-cdh4`` and ``hive-hadoop1`` connectors which
  support CDH 4 and Hadoop 1.x, respectively.
* Remove the ``hive-cdh5`` connector as an alias for ``hive-hadoop2``.
* Remove support for the legacy S3 block-based file system.
* Add support for KMS-managed keys for S3 server-side encryption.

Cassandra Changes
-----------------

* Add support for Cassandra 3.x by removing the deprecated Thrift interface used to
  connect to Cassandra. The following config options are now defunct and must be removed:
  ``cassandra.thrift-port``, ``cassandra.thrift-connection-factory-class``,
  ``cassandra.transport-factory-options`` and ``cassandra.partitioner``.

SPI Changes
-----------

* Add methods to ``SystemAccessControl`` and ``ConnectorAccessControl`` to
  filter the list of catalogs, schemas and tables.
* Add access control checks for :doc:`/sql/show-schemas` and :doc:`/sql/show-tables`.
* Add ``beginQuery`` and ``cleanupQuery`` notifications to ``ConnectorMetadata``.
