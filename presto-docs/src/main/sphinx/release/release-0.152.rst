=============
Release 0.152
=============

General Changes
---------------

* Add :func:`array_union` function.
* Add :func:`reverse` function for arrays.
* Fix issue that could cause queries with ``varchar`` literals to fail.
* Fix categorization of errors from :func:`url_decode`, allowing it to be used with ``TRY``.
* Fix error reporting for invalid JSON paths provided to JSON functions.
* Fix view creation for queries containing ``GROUPING SETS``.
* Fix query failure when referencing a field of a ``NULL`` row.
* Improve query performance for multiple consecutive window functions.
* Prevent web UI from breaking when query fails without an error code.
* Display port on the task list in the web UI when multiple workers share the same host.
* Add support for ``EXCEPT``.
* Rename ``FLOAT`` type to ``REAL`` for better compatibility with the SQL standard.
* Fix potential performance regression when transporting rows between nodes.

JDBC Driver Changes
-------------------

* Fix sizes returned from ``DatabaseMetaData.getColumns()`` for
  ``COLUMN_SIZE``, ``DECIMAL_DIGITS``, ``NUM_PREC_RADIX`` and ``CHAR_OCTET_LENGTH``.

Hive Changes
------------

* Fix resource leak in Parquet reader.
* Rename JMX stat ``AllViews`` to ``GetAllViews`` in ``ThriftHiveMetastore``.
* Add file based security, which can be configured with the ``hive.security``
  and ``security.config-file`` config properties. See :doc:`/connector/hive-security`
  for more details.
* Add support for custom S3 credentials providers using the
  ``presto.s3.credentials-provider`` Hadoop configuration property.

MySQL Changes
-------------

* Fix reading MySQL ``tinyint(1)`` columns. Previously, these columns were
  incorrectly returned as a boolean rather than an integer.
* Add support for ``INSERT``.
* Add support for reading data as ``tinyint`` and ``smallint`` types rather than ``integer``.

PostgreSQL Changes
------------------

* Add support for ``INSERT``.
* Add support for reading data as ``tinyint`` and ``smallint`` types rather than ``integer``.

SPI Changes
-----------

* Remove ``owner`` from ``ConnectorTableMetadata``.
* Replace the  generic ``getServices()`` method in ``Plugin`` with specific
  methods such as ``getConnectorFactories()``, ``getTypes()``, etc.
  Dependencies like ``TypeManager`` are now provided directly rather
  than being injected into ``Plugin``.
* Add first-class support for functions in the SPI. This replaces the old
  ``FunctionFactory`` interface. Plugins can return a list of classes from the
  ``getFunctions()`` method:

  * Scalar functions are methods or classes annotated with ``@ScalarFunction``.
  * Aggregation functions are methods or classes annotated with ``@AggregationFunction``.
  * Window functions are an implementation of ``WindowFunction``. Most implementations
    should be a subclass of ``RankingWindowFunction`` or ``ValueWindowFunction``.

.. note::
    This is a backwards incompatible change with the previous SPI.
    If you have written a plugin, you will need to update your code
    before deploying this release.

Verifier Changes
----------------

* Fix handling of shadow write queries with a ``LIMIT``.

Local File Changes
------------------

* Fix file descriptor leak.
