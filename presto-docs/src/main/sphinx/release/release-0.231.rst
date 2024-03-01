=============
Release 0.231
=============

.. warning::

   There is a bug in this release that will cause queries with the predicate ``IS NULL`` on
   bucketed columns to produce incorrect results.

General Changes
_______________
* Fix failures caused by invalid plans for queries with repeated lambda expressions in the order by clause.
* Fix accounting of raw input bytes in ScanFilterAndProjectOperator.
* Fix an issue where server fails to start when two function namespace managers of the same type are specified.
* Fix SQL function compilation error when input parameters contain lambda.
* Fix ``IS DISTINCT FROM`` for long decimals.
* Improve error handling for geometry deserialization edge cases.
* Improve performance of :func:`array_join`.
* Improve efficiency of :func:`ST_Buffer`. The new implementation produces fewer buffer points on rounded corners, which will produce very similar but different results. It also better handles buffering with small (<1e-9) distances.
* Improve OOM killer log output to put memory heavy nodes and queries first.
* Improve efficiency by using JTS instead of Esri for many geometrical operations and fix incorrect calculation of extreme points in certain cases (:issue:`14031`).
* Add forms of :func:`approx_percentile` accepting an accuracy parameter.
* Add a new session property ``aggregation_partitioning_merging_strategy`` to configure partition merging strategy when adding exchange around aggregation node (:pr:`11262`).
* Add ``IGNORE NULLS`` clause to various :doc:`/functions/window`.

Hive Changes
____________
* Fix schema mismatch with Parquet ``INT64`` & ``Timestamp``.
* Improve string column stats by including the sum of all strings' lengths when the individual min/max of the string is too long in ORC writer.
* Allow reading data from HDFS while caching the fetched data on local storage. Turn on the feature by specifying the cache directory config ``cache.base-directory``.
* Add support for parallel partition fetching for the Glue metastore.
* Add support for the AWS Default Credentials Provider for S3.

Raptor Changes
______________
* Add the ability to read and write with delta deletes.

Verifier Changes
________________
* Fix an issue where checksum query text and ID are not recorded if the checksum query fails.
* Improve verifier to skip verification when checksum query fails to compile.
* Add checks for the cardinalities sum when validating an array column.
* Add new columns ``control_session_properties`` and ``test_session_properties`` to ``verifier_queries``, and remove column ``session_properties_json``. The value of the removed column can be copied to the two new columns for the schema change.
* Add details of determinism analysis runs to the output.
* Add configuration property ``max-determinism-analysis-runs`` to control maximum number of determinism analysis runs in case of column mismatch.
* Add configuration property ``run-teardown-for-determinism-analysis`` to allow disabling teardown for determinism analysis runs.

SPI Changes
___________
* Add API to check if the query is unexpectedly modified using the credentials passed in the identity.
