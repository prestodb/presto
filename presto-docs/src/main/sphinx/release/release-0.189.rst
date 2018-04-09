=============
Release 0.189
=============

General Changes
---------------

* Fix query failure while logging the query plan.
* Fix a bug that causes clients to hang when executing ``LIMIT`` queries when
  ``optimizer.force-single-node-output`` is disabled.
* Fix a bug in the :func:`bing_tile_at` and :func:`bing_tile_polygon` functions
  where incorrect results were produced for points close to tile edges.
* Fix variable resolution when lambda argument has the same name as a table column.
* Improve error message when running ``SHOW TABLES`` on a catalog that does not exist.
* Improve performance for queries with highly selective filters.
* Execute :doc:`/sql/use` on the server rather than in the CLI, allowing it
  to be supported by any client. This requires clients to add support for
  the protocol changes (otherwise the statement will be silently ignored).
* Allow casting ``JSON`` to ``ROW`` even if the ``JSON`` does not contain every
  field in the ``ROW``.
* Add support for dereferencing row fields in lambda expressions.

Security Changes
----------------

* Support configuring multiple authentication types, which allows supporting
  clients that have different authentication requirements or gracefully
  migrating between authentication types without needing to update all clients
  at once. Specify multiple values for ``http-server.authentication.type``,
  separated with commas.
* Add support for TLS client certificates as an authentication mechanism by
  specifying ``CERTIFICATE`` for ``http-server.authentication.type``.
  The distinguished name from the validated certificate will be provided as a
  ``javax.security.auth.x500.X500Principal``. The certificate authority (CA)
  used to sign client certificates will be need to be added to the HTTP server
  KeyStore (should technically be a TrustStore but separating them out is not
  yet supported).
* Skip sending final leg of SPNEGO authentication when using Kerberos.

JDBC Driver Changes
-------------------

* Per the JDBC specification, close the ``ResultSet`` when ``Statement`` is closed.
* Add support for TLS client certificate authentication by configuring the
  ``SSLKeyStorePath`` and ``SSLKeyStorePassword`` parameters.
* Add support for transactions using SQL statements or the standard JDBC mechanism.
* Allow executing the ``USE`` statement. Note that this is primarily useful when
  running arbitrary SQL on behalf of users. For programmatic use, continuing
  to use ``setCatalog()`` and ``setSchema()`` on ``Connection`` is recommended.
* Allow executing ``SET SESSION`` and ``RESET SESSION``.

Resource Group Changes
----------------------

* Add ``WEIGHTED_FAIR`` resource group scheduling policy.

Hive Changes
------------

* Do not require setting ``hive.metastore.uri`` when using the file metastore.
* Reduce memory usage when reading string columns from ORC or DWRF files.


MySQL, PostgreSQL, Redshift, and SQL Server Changes
---------------------------------------------------

* Change mapping for columns with ``DECIMAL(p,s)`` data type from Presto ``DOUBLE``
  type to the corresponding Presto ``DECIMAL`` type.

Kafka Changes
-------------

* Fix documentation for raw decoder.

Thrift Connector Changes
------------------------

* Add support for index joins.

SPI Changes
-----------

* Deprecate ``SliceArrayBlock``.
* Add ``SessionPropertyConfigurationManager`` plugin to enable overriding default
  session properties dynamically.
