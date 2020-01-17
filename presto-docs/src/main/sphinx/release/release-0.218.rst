=============
Release 0.218
=============

.. warning::

    This release has the potential to produce incorrect results for three way joins where one of the joins is a ``FULL OUTER JOIN`` and one subquery
    of the ``FULL OUTER JOIN`` has a ``GROUP BY`` on the join key and some expression that could be evaluated to a constant. For details, please
    refer to :issue:`12577`.

General Changes
---------------

* Fix failures in regular expression functions for certain inputs where the pattern contains word boundaries (e.g. ``\b``).
* Fix an issue that may cause a crash when using plugins that provide an event listener. (:issue:`11951`)
* Fix a memory leak that occurs when a query fails with a semantic or permission error.
* Improve performance for queries with ``FULL OUTER JOIN`` where join keys have the :func:``COALESCE`` function applied.
* Improve cost based optimizer to make decisions based on estimated query peak memory.
* Improve cost based optimizer for certain queries using ``ORDER BY``.
* Improve performance for queries with an ``OUTER JOIN`` followed by ``LIMIT``.
* Improve the error message for ``INSERT`` queries where columns do not match the target table.
* Add support for using binary encoding for coordinator-to-worker communication.
  This feature is experimental, and it can be enabled with the ``experimental.internal-communication.binary-transport-enabled`` configuration property.
  Enabling this feature may help with coordinator scalability and reduces network, CPU, and memory usage on the coordinator.
* Add :func:`ST_Area` for the ``SphericalGeography`` type.
* Add a system table ``system.metadata.analyze_properties`` that shows the properties supported by the ``ANALYZE`` statement.
* Add support for resolving key conflicts when using :func:`split_to_map`.
* Add support for role management (see :doc:`/sql/create-role`). Client library version 0.218 is required to use :doc:`/sql/set-role`. (:issue:`11645`)
* Add support for processing JSON protocol messages by generating bytecode on the coordinator.
  This feature is experimental, and it can be enabled with the ``experimental.json-serde-codegen-enabled`` configuration property.


Security Changes
----------------

* Change principal hostname to be configurable in Kerberos authenticator.


Hive Connector Changes
----------------------

* Improve Parquet reader performance by reducing redundant footer reads.
* Add support for skipping Glacier files in Amazon S3. This feature can be enabled by setting the ``hive.s3.skip-glacier-objects`` configuration property.
* Add support for Parquet files written with Parquet v1.9+ that use ``DELTA_BINARY_PACKED`` encoding with the ``INT64`` type.
* Add support for dictionary filtering for Parquet v2 files that use ``RLE_DICTIONARY`` encoding.


Elasticsearch Connector Changes
-------------------------------

* Add support for Search Guard in Elasticsearch connector. Please refer to :doc:`/connector/elasticsearch` for
  the relevant configuration properties.


MySQL Connector Changes
-----------------------

* Allow creating or renaming tables, and adding, renaming, or dropping columns.


PostgreSQL Connector Changes
----------------------------

* Allow creating or renaming tables, and adding, renaming, or dropping columns.


Redshift Connector Changes
--------------------------

* Allow creating or renaming tables, and adding, renaming, or dropping columns.


SQL Server Connector Changes
----------------------------

* Allow creating or renaming tables, and adding, renaming, or dropping columns.


SPI Changes
-----------

* Add ``Connector.getCapabilities()`` to allow connectors to individually opt-in to connector-specific functionality.