=============
Release 0.230
=============

General Changes
_______________
* Add support to list non-builtin functions in SHOW FUNCTIONS. The feature can be turned on by the configuration property ``list-non-built-in-functions``.
* Fix SQL function byte code compilation error for functions implemented with constants.
* Add support for ``DROP FUNCTION``.
* Fix SQL function execution not respecting null-call clause.
* Fix regression on lambda evaluation (Issue#13648).
* Added combinations function, a function that returns n combinations of values in an array, up to n=5.
* Fix SQL function compilation failure when the function parameter is not referenced in function body.
* Add all_match(), any_match(), and none_match() functions.
* Improved PRESTO_EXTRA_CREDENTIAL header parsing to allow value contain multiple '=' and urlEncode characters.
* Fix geometry_to_bing_tile for geometries at -180 longitude or 85.05112878 latitude.
* Fix compilation errors for expressions over types containing an extremely large number of nested types.
* Add expand_envelope function to return a geometry's envelope expanded by a distance.

Verifier Changes
________________
* Add skipped verification results to the output for queries being filtered.

SPI Changes
___________
* Change ``ConnectorMetadata#commitPartition`` into async operation, and rename it to ``ConnectorMetadata#commitPartitionAsync``.

Hive Changes
____________
* Add an Alluxio client jar to `plugin/hive-hadoop2/` (as a runtime dependency) to avoid copying Alluxio client jar to all Presto servers manually to connect to Alluxio.
* Improve BatchStreamReader performance.
* Metastore interface is separated into a separate module to reduce monolithicness.

Raptor Changes
______________
* Add table_supports_delta_delete property in Raptor to allow deletion happening in background. DELETE queries in Raptor can now delete data logically but relying on compactors to delete physical data.
* Introduced new cache module to allow using local disk for to cache files from remote file systems.
* Allow Raptor to read data from HDFS while caching the files on local disks.
* Fix organizer not running scheduled job due to misreading `storage.organization-discovery-interval` config.
