=============
Release 0.203
=============

General Changes
---------------

* Fix spurious duplicate key errors from :func:`map`.
* Fix planning failure when a correlated subquery containing a ``LIMIT``
  clause is used within ``EXISTS`` (:issue:`x10696`).
* Fix out of memory error caused by missing pushback checks in data exchanges.
* Fix execution failure for queries containing a cross join when using bucketed execution.
* Fix execution failure for queries containing an aggregation function
  with ``DISTINCT`` and a highly selective aggregation filter.
  For example: ``sum(DISTINCT x) FILTER (WHERE y = 0)``
* Fix quoting in error message for ``SHOW PARTITIONS``.
* Eliminate redundant calls to check column access permissions.
* Improve query creation reliability by delaying query start until the client
  acknowledges the query ID by fetching the first response link. This eliminates
  timeouts during the initial request for queries that take a long time to analyze.
* Remove support for legacy ``ORDER BY`` semantics.
* Distinguish between inner and left spatial joins in explain plans.

Security Changes
----------------

* Fix sending authentication challenge when at least two of the
  ``KERBEROS``, ``PASSWORD``, or ``JWT`` authentication types are configured.
* Allow using PEM encoded (PKCS #8) keystore and truststore with the HTTP server
  and the HTTP client used for internal communication. This was already supported
  for the CLI and JDBC driver.

Server RPM Changes
------------------

* Declare a dependency on ``uuidgen``. The ``uuidgen`` program is required during
  installation of the Presto server RPM package and lack of it resulted in an invalid
  config file being generated during installation.

Hive Connector Changes
----------------------

* Fix complex type handling in the optimized Parquet reader. Previously, null values,
  optional fields, and Parquet backward compatibility rules were not handled correctly.
* Fix an issue that could cause the optimized ORC writer to fail with a ``LazyBlock`` error.
* Improve error message for max open writers.

Thrift Connector Changes
------------------------

* Fix retry of requests when the remote Thrift server indicates that the
  error is retryable.

Local File Connector Changes
----------------------------

* Fix parsing of timestamps when the JVM time zone is UTC (:issue:`x9601`).
