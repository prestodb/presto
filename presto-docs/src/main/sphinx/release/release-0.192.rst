=============
Release 0.192
=============

General Changes
---------------

* Fix correctness issue in the :func:`geometry_to_bing_tiles` function that causes
  it to return irrelevant tiles when bottom or right side of the bounding box of the
  geometry is aligned with the tile border.
* Fix handling of invalid WKT (well-known text) input in geospatial functions.
* Fix an issue that can cause long-running queries to hang when writer scaling is enabled.
* Fix cast from ``REAL`` or ``DOUBLE`` to ``DECIMAL`` to conform to the SQL standard.
  For example, previously ``cast (double '100000000000000000000000000000000' as decimal(38))``
  would return ``100000000000000005366162204393472``. Now it returns ``100000000000000000000000000000000``.
* Fix incorrect reporting of input size and positions in live plan view.
* Fix bug in validation of resource groups that prevented use of the ``WEIGHTED_FAIR`` policy.
* Fix HTTP 500 errors from the coordinator due to upstream errors when fetching data from workers.
* Improve memory tracking for queries involving DISTINCT or :func:`row_number` that could cause
  over-committing memory resources for short time periods.
* Improve performance for queries involving ``GROUPING``.
* Improve buffer utilization calculation for writer scaling.
* Remove tracking of per-driver peak memory reservation.
* Add ``resource-groups.max-refresh-interval`` config option to limit the maximum acceptable
  staleness of resource group configuration.
* Remove ``dictionary-processing-joins-enabled`` configuration option and ``dictionary_processing_join``
  session property.

Hive changes
------------

* Fix reading partitioned table statistics in Hive 2.0.
* Do not treat file system errors as corruptions for ORC.
* Prevent reads from tables or partitions with ``object_not_readable`` attribute set.
* Add support for validating ORC files after they have been written. This behavior can
  be turned on via the ``hive.orc.writer.validate`` configuration property.
* Expose ORC writer statistics via JMX.
* Add configurations options to control ORC writer minimum/maximum rows per stripe and row group,
  maximum stripe size and memory limit for dictionaries.
* Allow reading empty ORC files.
* Handle ViewFs when checking file system cache expiration.
* Improve error reporting when the target table of an insert query is dropped.
* Remove retry when creating Hive RecordReader. This can help queries fail faster.

MySQL changes
-------------

* Remove support for ``TIME WITH TIME ZONE`` and ``TIMESTAMP WITH TIME ZONE``
  types due to MySQL types not being able to store timezone information.
* Add support for ``REAL`` type, which maps to MySQL's ``FLOAT`` type.

PostgreSQL changes
------------------

* Add support for ``VARBINARY`` type, which is maps to PostgreSQL's ``BYTEA`` type.

MongoDB changes
---------------

* Fix support for pushing down inequality operators for string types.
* Add support for reading documents as ``MAP`` values.
* Add support for MongoDB's ``Decimal128`` type.
* Treat document and array of document as ``JSON`` instead of ``VARCHAR``.

CLI Changes
-----------

* Fix update of prompt after ``USE`` statement.
* Fix correctness issue when rendering arrays of bing tiles that causes
  the first entry to be repeated multiple times.

JMX Changes
-----------

* Allow nulls in history table values.

SPI Changes
-----------

* Remove ``SliceArrayBlock`` class.
* Add ``offset`` and ``length`` to ``Block::getPositions``.
