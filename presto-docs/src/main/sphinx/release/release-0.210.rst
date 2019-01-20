=============
Release 0.210
=============

General Changes
---------------

* Fix planning failure when aliasing columns of tables containing hidden
  columns (:issue:`x11385`).
* Fix correctness issue when ``GROUP BY DISTINCT`` terms contain references to
  the same column using different syntactic forms (:issue:`x11120`).
* Fix failures when querying ``information_schema`` tables using capitalized names.
* Improve performance when converting between ``ROW`` types.
* Remove user CPU time tracking as introduces non-trivial overhead.
* Select join distribution type automatically for queries involving outer joins.

Hive Connector Changes
----------------------

* Fix a security bug introduced in 0.209 when using ``hive.security=file``,
  which would allow any user to create, drop, or rename schemas.
* Prevent ORC writer from writing stripes larger than the max configured size
  when converting a highly dictionary compressed column to direct encoding.
* Support creating Avro tables with a custom schema using the ``avro_schema_url``
  table property.
* Support backward compatible Avro schema evolution.
* Support cross-realm Kerberos authentication for HDFS and Hive Metastore.

JDBC Driver Changes
-------------------

* Deallocate prepared statement when ``PreparedStatement`` is closed. Previously,
  ``Connection`` became unusable after many prepared statements were created.
* Remove ``getUserTimeMillis()`` from ``QueryStats`` and ``StageStats``.

SPI Changes
-----------

* ``SystemAccessControl.checkCanSetUser()`` now takes an ``Optional<Principal>``
  rather than a nullable ``Principal``.
* Rename ``connectorId`` to ``catalogName`` in ``ConnectorFactory``,
  ``QueryInputMetadata``, and ``QueryOutputMetadata``.
* Pass ``ConnectorTransactionHandle`` to ``ConnectorAccessControl.checkCanSetCatalogSessionProperty()``.
* Remove ``getUserTime()`` from ``SplitStatistics`` (referenced in ``SplitCompletedEvent``).

.. note::
    These are backwards incompatible changes with the previous SPI.
    If you have written a plugin, you will need to update your code
    before deploying this release.
