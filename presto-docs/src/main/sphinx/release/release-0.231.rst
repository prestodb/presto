=============
Release 0.231
=============

General Changes
_______________
* Add support for get partition names by filter.
* Add forms of approx_percentile accepting an accuracy parameter.
* Improve error handling for geometry deserialization edge cases.
* Fix failures caused by invalid plans for queries with repeated lambda expressions in the order by clause.
* Fix ScanFilterAndProjectOperator raw input bytes accounting for LazyBlocks.
* Output (sum) string stat even if min/max values are too long. This is needed for the read-path to be able to better estimate the size of row.
* Get PrestoS3FileSystem to work with the AWS Default Credentials Provider.
* Optimizer performance for array_join.
* Use more efficient implementation for ST_Buffer.  This produces fewer buffer points on rounded corners, which will produce very similar but different results.  JTS also better handles buffering with small (<1e-9) distances.
* Fix an issue where server fails to start when two function namespace managers of the same type are specified.
* OOM killer log output sorted to put memory heavy nodes and queries first.
* Use JTS instead of Esri for many geometrical operations. + Polygon WKTs must have closed loops.  Previously Esri would close the loops for you. + Certain other invalid geometries will fail to be created from WKTs, such as `LINESTRING(0 0, 0 0, 0 0)`. + Returned WKTs may have a different point order. + Fixes incorrect calculation of extreme points in certain cases.
* Fix SQL function compilation error when input parameters contain lambda.

Verifier Changes
________________
* Add checks for the cardinalities sum when validating an array column.
* Improve Verifier to skip verification when checksum query fails to compile.
* Fix an issue where checksum query text and ID are not recorded if the checksum query fails.
* Add new columns ``control_session_properties`` and `test_session_properties` to ``verifier_queries``, and remove column ``session_properties_json``. The value of the removed column can be copied to the two new columns for the schema change.
* Add details of determinism analysis runs to the output.
* Add configuration property ``max-determinism-analysis-runs`` to control maximum number of determinism analysis runs in case of column mismatch.
* Add configuration property ``run-teardown-for-determinism-analysis`` to allow disabling teardown for determinism analysis runs.

SPI Changes
___________
* Add API to check if the query is unexpectedly modified using the credentials passed in the identity.

Hive Changes
____________
* Allow reading data from HDFS while caching the fetched data on local disks. Turn on the feature by specifying the cache directory config `cache.base-directory`.
* Add support for parallel partition fetching for the Glue metastore.

Parquet Changes
_______________
* Fix schema mismatch w/Parquet INT64 & Timestamp.

Raptor Changes
______________
* Add the ability to read and write with delta deletes.
