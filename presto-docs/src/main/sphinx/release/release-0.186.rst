=============
Release 0.186
=============

.. warning::

    This release has a stability issue that may cause query failures in large deployments
    due to HTTP requests timing out.

General Changes
---------------

* Fix excessive GC overhead caused by map to map cast.
* Fix implicit coercions for ``ROW`` types, allowing operations between
  compatible types such as ``ROW(INTEGER)`` and ``ROW(BIGINT)``.
* Fix issue that may cause queries containing expensive functions, such as regular
  expressions, to continue using CPU resources even after they are killed.
* Fix performance issue caused by redundant casts.
* Fix :func:`json_parse` to not ignore trailing characters. Previously,
  input such as ``[1,2]abc`` would successfully parse as ``[1,2]``.
* Fix leak in running query counter for failed queries. The counter would
  increment but never decrement for queries that failed before starting.
* Reduce coordinator HTTP thread usage for queries that are queued or waiting for data.
* Reduce memory usage when building data of ``VARCHAR`` or ``VARBINARY`` types.
* Estimate memory usage for ``GROUP BY`` more precisely to avoid out of memory errors.
* Add queued time and elapsed time to the client protocol.
* Add ``query_max_execution_time`` session property and ``query.max-execution-time`` config
  property. Queries will be aborted after they execute for more than the specified duration.
* Add :func:`inverse_normal_cdf` function.
* Add :doc:`/functions/geospatial` including functions for processing Bing tiles.
* Add :doc:`/admin/spill` for joins.
* Add :doc:`/connector/redshift`.

Resource Groups Changes
-----------------------

* Query Queues are deprecated in favor of :doc:`/admin/resource-groups`
  and will be removed in a future release.
* Rename the ``maxRunning`` property to ``hardConcurrencyLimit``. The old
  property name is deprecated and will be removed in a future release.
* Fail on unknown property names when loading the JSON config file.

JDBC Driver Changes
-------------------

* Allow specifying an empty password.
* Add ``getQueuedTimeMillis()`` and ``getElapsedTimeMillis()`` to ``QueryStats``.

Hive Changes
------------

* Fix ``FileSystem closed`` errors when using Kerberos authentication.
* Add support for path style access to the S3 file system. This can be enabled
  by setting the ``hive.s3.path-style-access=true`` config property.

SPI Changes
-----------

* Add an ``ignoreExisting`` flag to ``ConnectorMetadata::createTable()``.
* Remove the ``getTotalBytes()`` method from ``RecordCursor`` and ``ConnectorPageSource``.

.. note::

    These are backwards incompatible changes with the previous SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
