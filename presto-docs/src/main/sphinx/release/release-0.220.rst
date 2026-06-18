=============
Release 0.220
=============

General Changes
---------------
* Fix an issue where spatial joins executed after right outer joins would fail in certain cases. See :issue:`12672`.
* Fix an issue where ``TRY`` cannot catch the failures in ``ROW`` comparisons with ``NULL`` fields and ``ARRAY`` comparisons with ``NULL`` elements.
* Improve scan performance for queries that read map columns but do not access individual keys.

Raptor Changes
--------------
* Add support for writing files with ``ZSTD`` compression. Add configuration property ``storage.orc.compression-kind`` to switch between ``SNAPPY`` and ``ZSTD``.
  This is only applied when optimized ORC writer is turned on.
* Improve performance of the ORC writer in Raptor. Add configuration property ``storage.orc.optimized-writer-stage`` to enable the performance enhancement.
  The optimized ORC writer is backward compatible with the Hadoop ORC writer, but the Hadoop ORC writer is not forward compatible with it.
  Turning on the optimized ORC writer should work on an existing Raptor server. However, switching back to the Hadoop ORC writer may fail Raptor background job.


Hive Connector Changes
----------------------
* Add ``compression_codec`` session property to set the compression codec when writing files. Possible values are ``NONE``, ``SNAPPY``, and ``GZIP``.
* Add experimental support for partial merge pushdown, which optimizes plans for queries joining over two tables with mismatched but compatible bucket counts.
  This feature can be enabled by session property ``experimental.optimizer.partial-merge-pushdown-strategy``, and is aiming to supersede ``hive.optimize-mismatched-bucket-count``.
  See :issue:`12611`.
* Improve query performance when writing bucketed tables.


MongoDB Connector Changes
-------------------------
* Add ``mongodb.read-preference-tags`` configuration property to configure mongodb read preference tags.
  Tag sets are separated by an ampersand, and each tag set is specified as a comma-separated list of colon-separated key-value pairs.
  For example, ``mongodb.read-preference-tags=dc:east,use:reporting&use:reporting``.

JDBC Driver Changes
-------------------
* Fix incorrect precision and column display size in ``ResultSetMetaData`` for ``CHAR`` and ``VARCHAR`` columns.

Verifier Changes
----------------
* Fix an issue where Verifier would always mark floating point columns as mismatched if either the control checksum or test checksum was 0 while the other was close to 0.
  Add configuration property ``absolute-error-margin`` to configure this threshold.
* Fix error messages for floating point column mismatches.
* Add configuration property ``failure-resolver.enabled`` to allow automatic failure resolution be disabled.
* Add control and test checksum query IDs to Verifier output event.
* Improve Presto query retries by allowing Verifier to recognize ``JdbcErrorCode`` and ``ThriftErrorCode``,
  and by marking ``ABANDONED_TASK``, ``HIVE_WRITER_OPEN_ERROR``, ``JDBC_ERROR``, and ``THRIFT_SERVICE_CONNECTION_ERROR`` as retryable errors.

SPI Changes
-----------
* Move ``FunctionMetadata`` to SPI.
* Add ``FunctionMetadataManager`` and ``StandardFunctionResolution`` in ``ConnectorContext``.
  The two interfaces allow connectors to obtain the metadata of a Presto system function as well as to resolve standard functions, such as add, minus, and, or, etc.
* Remove ``distributedPlanningTime`` from ``QueryStatistics``
* Remove redundant offset parameter in ``Block#getByte``, ``Block#getShort``, and ``Block#getInt``.
