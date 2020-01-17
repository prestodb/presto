=============
Release 0.195
=============

General Changes
---------------

* Fix :func:`histogram` for map type when type coercion is required.
* Fix ``nullif`` for map type when type coercion is required.
* Fix incorrect termination of queries when the coordinator to worker communication is under high load.
* Fix race condition that causes queries with a right or full outer join to fail.
* Change reference counting for varchar, varbinary, and complex types to be approximate. This
  approximation reduces GC activity when computing large aggregations with these types.
* Change communication system to be more resilient to issues such as long GC pauses or networking errors.
  The min/max sliding scale of for timeouts has been removed and instead only max time is used.
  The ``exchange.min-error-duration`` and ``query.remote-task.min-error-duration`` are now ignored and will be
  removed in a future release.
* Increase coordinator timeout for cleanup of worker tasks for failed queries.  This improves the health of
  the system when workers are offline for long periods due to GC or network errors.
* Remove the ``compiler.interpreter-enabled`` config property.

Security Changes
----------------

* Presto now supports generic password authentication using a pluggable :doc:`/develop/password-authenticator`.
  Enable password authentication by setting ``http-server.authentication.type`` to include ``PASSWORD`` as an
  authentication type.
* :doc:`/security/ldap` is now implemented as a password authentication
  plugin. You will need to update your configuration if you are using it.

CLI and JDBC Changes
--------------------

* Provide a better error message when TLS client certificates are expired or not yet valid.

MySQL Changes
-------------

* Fix an error that can occur while listing tables if one of the listed tables is dropped.

Hive Changes
------------

* Add support for LZ4 compressed ORC files.
* Add support for reading Zstandard compressed ORC files.
* Validate ORC compression block size when reading ORC files.
* Set timeout of Thrift metastore client. This was accidentally removed in 0.191.

MySQL, Redis, Kafka, and MongoDB Changes
----------------------------------------

* Fix failure when querying ``information_schema.columns`` when there is no equality predicate on ``table_name``.
