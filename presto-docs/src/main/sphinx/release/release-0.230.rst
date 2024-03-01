=============
Release 0.230
=============

General Changes
_______________
* Fix an issue where a SQL function fails to compile when the function implementation contains constants.
* Fix an issue where a SQL function fails to compile when some function parameters are not referenced in the function body.
* Fix an issue where SQL functions do not respect behavior specification for null arguments during execution.
* Fix compilation errors for expressions over types containing an extremely large number of nested types (:pr:`13405`).
* Fix a regression in lambda evaluation (:issue:`13648`).
* Fix :func:`geometry_to_bing_tiles` function for geometries at -180 longitude or 85.05112878 latitude.
* Improve ``PRESTO_EXTRA_CREDENTIAL`` header parsing to allow for values contain multiple ``=`` characters and url-encoded characters.
* Add support to list non-builtin functions in SHOW FUNCTIONS. The feature can be turned on by the configuration property ``list-non-built-in-functions``.
* Add support for ``DROP FUNCTION``.
* Add :func:`combinations` function, which returns ``n`` combinations of values in an array, up to ``n = 5``.
* Add :func:`all_match()`, :func:`any_match()`, and :func:`none_match()` functions.
* Add :func:`expand_envelope` function to return a geometry's envelope expanded by a distance.

Hive Changes
____________
* Add Alluxio client jar to ``plugin/hive-hadoop2/`` as a runtime dependency to avoid having to copy the jar file to all Presto servers manually when connecting to Alluxio.
* Improve ORC reader performance.
* Add configuration property ``hive.zstd-jni-decompression-enabled`` to enable JNI ZSTD decompressor for ORC files.

Raptor Changes
______________
* Fix an issue where the organizer does not run scheduled jobs.
* Add support for caching data read from HDFS on a local disk.
* Add configuration property ``storage.zstd-jni-decompression-enabled`` to enable JNI ZSTD decompressor for ORC files.

Verifier Changes
________________
* Add support to always output verification results for queries being skipped for verification.

SPI Changes
___________
* Replace ``ConnectorMetadata#commitPartition`` with async operation ``ConnectorMetadata#commitPartitionAsync``.
