=============
Release 0.192
=============

General Changes
---------------

* Fix performance regression in split scheduling introduced in 0.191. If a query
  scans a non-trivial number of splits (~1M splits in an hour), the coordinator
  CPU utilization can be very high, leading to elevated communication failures.
* Fix correctness issue in the :func:`geometry_to_bing_tiles` function that causes
  it to return irrelevant tiles when bottom or right side of the bounding box of the
  geometry is aligned with the tile border.
* Fix handling of invalid WKT (well-known text) input in geospatial functions.
* Fix an issue that can cause long-running queries to hang when writer scaling is enabled.
* Fix cast from ``REAL`` or ``DOUBLE`` to ``DECIMAL`` to conform to the SQL standard.
  For example, previously ``cast (double '100000000000000000000000000000000' as decimal(38))``
  would return ``100000000000000005366162204393472``. Now it returns ``100000000000000000000000000000000``.
* Fix bug in validation of resource groups that prevented use of the ``WEIGHTED_FAIR`` policy.
* Fail queries properly when the coordinator fails to fetch data from workers.
  Previously, it would return an HTTP 500 error to the client.
* Improve memory tracking for queries involving ``DISTINCT`` or :func:`row_number` that could cause
  over-committing memory resources for short time periods.
* Improve performance for queries involving ``grouping()``.
* Improve buffer utilization calculation for writer scaling.
* Remove tracking of per-driver peak memory reservation.
* Add ``resource-groups.max-refresh-interval`` config option to limit the maximum acceptable
  staleness of resource group configuration.
* Remove ``dictionary-processing-joins-enabled`` configuration option and ``dictionary_processing_join``
  session property.

Web UI Changes
--------------

* Fix incorrect reporting of input size and positions in live plan view.

CLI Changes
-----------

* Fix update of prompt after ``USE`` statement.
* Fix correctness issue when rendering arrays of Bing tiles that causes
  the first entry to be repeated multiple times.

Hive Changes
------------

* Fix reading partitioned table statistics from newer Hive metastores.
* Do not treat file system errors as corruptions for ORC.
* Prevent reads from tables or partitions with ``object_not_readable`` attribute set.
* Add support for validating ORC files after they have been written. This behavior can
  be turned on via the ``hive.orc.writer.validate`` configuration property.
* Expose ORC writer statistics via JMX.
* Add configuration options to control ORC writer min/max rows per stripe and row group,
  maximum stripe size, and memory limit for dictionaries.
* Allow reading empty ORC files.
* Handle ViewFs when checking file system cache expiration.
* Improve error reporting when the target table of an insert query is dropped.
* Remove retry when creating Hive record reader. This can help queries fail faster.

MySQL Changes
-------------

* Remove support for ``TIME WITH TIME ZONE`` and ``TIMESTAMP WITH TIME ZONE``
  types due to MySQL types not being able to store timezone information.
* Add support for ``REAL`` type, which maps to MySQL's ``FLOAT`` type.

PostgreSQL Changes
------------------

* Add support for ``VARBINARY`` type, which maps to PostgreSQL's ``BYTEA`` type.

MongoDB Changes
---------------

* Fix support for pushing down inequality operators for string types.
* Add support for reading documents as ``MAP`` values.
* Add support for MongoDB's ``Decimal128`` type.
* Treat document and array of documents as ``JSON`` instead of ``VARCHAR``.

JMX Changes
-----------

* Allow nulls in history table values.

SPI Changes
-----------

* Remove ``SliceArrayBlock`` class.
* Add ``offset`` and ``length`` parameters to ``Block.getPositions()``.
