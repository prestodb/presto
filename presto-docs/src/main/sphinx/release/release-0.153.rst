=============
Release 0.153
=============

General Changes
---------------

* Fix incorrect results for grouping sets when ``task.concurrency`` is greater than one.
* Fix silent numeric overflow when casting ``INTEGER`` to large ``DECIMAL`` types.
* Fix issue where ``GROUP BY ()`` would produce no results if the input had no rows.
* Fix null handling in :func:`array_distinct` when applied to the ``array(bigint)`` type.
* Fix handling of ``-2^63`` as the element index for :func:`json_array_get`.
* Fix correctness issue when the input to ``TRY_CAST`` evaluates to null.
  For types such as booleans, numbers, dates, timestamps, etc., rather than
  returning null, a default value specific to the type such as
  ``false``, ``0`` or ``1970-01-01`` was returned.
* Fix potential thread deadlock in coordinator.
* Fix rare correctness issue with an aggregation on a single threaded right join when
  ``task.concurrency`` is ``1``.
* Fix query failure when casting a map with null values.
* Fix failure when view column names contain upper-case letters.
* Fix potential performance regression due to skew issue when
  grouping or joining on columns of the following types: ``TINYINT``,
  ``SMALLINT``, ``INTEGER``, ``BIGINT``, ``REAL``, ``DOUBLE``,
  ``COLOR``, ``DATE``, ``INTERVAL``, ``TIME``, ``TIMESTAMP``.
* Fix potential memory leak for delete queries.
* Fix query stats to not include queued time in planning time.
* Fix query completion event to log final stats for the query.
* Fix spurious log messages when queries are torn down.
* Remove broken ``%w`` specifier for :func:`date_format` and :func:`date_parse`.
* Improve performance of :ref:`array_type` when underlying data is dictionary encoded.
* Improve performance of outer joins with non-equality criteria.
* Require task concurrency and task writer count to be a power of two.
* Use nulls-last ordering for :func:`array_sort`.
* Validate that ``TRY`` is used with exactly one argument.
* Allow running Presto with early-access Java versions.
* Add :doc:`/connector/accumulo`.

Functions and Language Features
-------------------------------

* Allow subqueries in non-equality outer join criteria.
* Add support for :doc:`/sql/create-schema`, :doc:`/sql/drop-schema`
  and :doc:`/sql/alter-schema`.
* Add initial support for correlated subqueries.
* Add execution support for prepared statements.
* Add ``DOUBLE PRECISION`` as an alias for the ``DOUBLE`` type.
* Add :func:`typeof` for discovering expression types.
* Add decimal support to :func:`avg`, :func:`ceil`, :func:`floor`, :func:`round`,
  :func:`truncate`, :func:`abs`, :func:`mod` and :func:`sign`.
* Add :func:`shuffle` function for arrays.

Pluggable Resource Groups
-------------------------

Resource group management is now pluggable. A ``Plugin`` can
provide management factories via ``getResourceGroupConfigurationManagerFactories()``
and the factory can be enabled via the ``etc/resource-groups.properties``
configuration file by setting the ``resource-groups.configuration-manager``
property. See the ``presto-resource-group-managers`` plugin for an example
and :doc:`/admin/resource-groups` for more details.

Web UI Changes
--------------

* Fix rendering failures due to null nested data structures.
* Do not include coordinator in active worker count on cluster overview page.
* Replace buffer skew indicators on query details page with scheduled time skew.
* Add stage total buffer, pending tasks and wall time to stage statistics on query details page.
* Add option to filter task lists by status on query details page.
* Add copy button for query text, query ID, and user to query details page.

JDBC Driver Changes
-------------------

* Add support for ``real`` data type, which corresponds to the Java ``float`` type.

CLI Changes
-----------

* Add support for configuring the HTTPS Truststore.

Hive Changes
------------

* Fix permissions for new tables when using SQL-standard authorization.
* Improve performance of ORC reader when decoding dictionary encoded :ref:`map_type`.
* Allow certain combinations of queries to be executed in a transaction-ish manner,
  for example, when dropping a partition and then recreating it. Atomicity is not
  guaranteed due to fundamental limitations in the design of Hive.
* Support per-transaction cache for Hive metastore.
* Fail queries that attempt to rename partition columns.
* Add support for ORC bloom filters in predicate push down.
  This is can be enabled using the ``hive.orc.bloom-filters.enabled``
  configuration property or the ``orc_bloom_filters_enabled`` session property.
* Add new optimized RCFile reader.
  This can be enabled using the ``hive.rcfile-optimized-reader.enabled``
  configuration property or the ``rcfile_optimized_reader_enabled`` session property.
* Add support for the Presto ``real`` type, which corresponds to the Hive ``float`` type.
* Add support for ``char(x)`` type.
* Add support for creating, dropping and renaming schemas (databases).
  The filesystem location can be specified when creating a schema,
  which allows, for example, easily creating tables on S3.
* Record Presto query ID for tables or partitions written by Presto
  using the ``presto_query_id`` table or partition property.
* Include path name in error message when listing a directory fails.
* Rename ``allow-all`` authorization method to ``legacy``. This
  method is deprecated and will be removed in a future release.
* Do not retry S3 requests that are aborted intentionally.
* Set the user agent suffix for S3 requests to ``presto``.
* Allow configuring the user agent prefix for S3 requests
  using the ``hive.s3.user-agent-prefix`` configuration property.
* Add support for S3-compatible storage using the ``hive.s3.endpoint``
  and ``hive.s3.signer-type`` configuration properties.
* Add support for using AWS KMS with S3 as an encryption materials provider
  using the ``hive.s3.kms-key-id`` configuration property.
* Allow configuring a custom S3 encryption materials provider using the
  ``hive.s3.encryption-materials-provider`` configuration property.

JMX Changes
-----------

* Make name configuration for history tables case-insensitive.

MySQL Changes
-------------

* Optimize fetching column names when describing a single table.
* Add support for ``char(x)`` and ``real`` data types.

PostgreSQL Changes
------------------

* Optimize fetching column names when describing a single table.
* Add support for ``char(x)`` and ``real`` data types.
* Add support for querying materialized views.

Blackhole Changes
-----------------

* Add ``page_processing_delay`` table property.

SPI Changes
-----------

* Add ``schemaExists()`` method to ``ConnectorMetadata``.
* Add transaction to grant/revoke in ``ConnectorAccessControl``.
* Add ``isCoordinator()`` and ``getVersion()`` methods to ``Node``.
* Remove ``setOptionalConfig()`` method from ``Plugin``.
* Remove ``ServerInfo`` class.
* Make ``NodeManager`` specific to a connector instance.
* Replace ``ConnectorFactoryContext`` with ``ConnectorContext``.
* Use ``@SqlNullable`` for functions instead of ``@Nullable``.
* Use a whitelist model for plugin class loading. This prevents connectors
  from seeing any classes that are not part of the JDK (bootstrap classes)
  or the SPI.
* Update ``presto-maven-plugin``, which provides a Maven packaging and
  lifecycle for plugins, to validate that every SPI dependency is marked
  as ``provided`` scope and that only SPI dependencies use ``provided``
  scope. This helps find potential dependency and class loader issues
  at build time rather than at runtime.

.. note::
    These are backwards incompatible changes with the previous SPI.
    If you have written a plugin, you will need to update your code
    before deploying this release.
