=============
Release 0.289
=============

**Highlights**
==============
* Add support to read tables up to Delta Protocol Version 3,7 through the use of the new Delta-Kernel api. :pr:`22596`
* Fix an issue where ``array_distinct``, ``array_except``, ``array_intersect``, ``array_union``,  ``set_union``, and ``set_agg`` would return an error if the input was an array of rows with null fields or array of arrays with null elements.  These functions now use full ``IS DISTINCT FROM`` semantics for comparison. :pr:`22938`
* Fix default Parquet writer version to ``PARQUET_1_0``. Set the hive config ``hive.parquet.writer.version = PARQUET_2_0`` for old behavior. :pr:`23369`
* Add a REST API in Presto C++ worker to fetch worker stats in Prometheus Data Format. :pr:`22360`

**Details**
===========

General Changes
_______________
* Fix a bug in the hash code of -0 for double and real types when ``use-new-nan-definition`` is false. :pr:`23060`
* Fix incorrect results on queries with count over mixed aggregation :pr:`23013`
* Fix a bug where like predicates would only match a newline when there was no wildcard at the end. :pr:`23404`
* Fix equijoin over timestamp with timezone types. :pr:`23319`
* Fix resource group state info endpoint to return current values from the resource groups. :pr:`23178`
* Fix array_min/array_max when the first element recursively contains an inner array with a null element. :pr:`23323`
* Fix :func:`map_top_n` function to be deterministic by using keys to break ties. :pr:`22778`
* Fix stddev and variance functions to always return correct results when input is constant. :pr:`23447`
* Add an optimization to remove cross join when one side of inputs is a single row of constant values. The optimization is controlled by the session property ``remove_cross_join_with_constant_single_row_input`` (default is ``true```). :pr:`23081`
* Add configuration property ``warn-on-possible-nans`` and session property ``warn_on_possible_nans`` to produce a warning on division operations or comparison operations involving double or real types. Division operations are common causes of accidental creation of NaNs, and the semantics of comparison operations involving NaNs changed considerably in the most recent Presto release. :pr:`23059`
* Add function :func:`ip_prefix_collapse`. :pr:`23445`
* Add function :func:`array_split_into_chunks`. :pr:`23264`
* Add a warning when an ``IGNORE NULL``` clause is used on any non lag, lead, first, last, or nth value function.  In future releases these queries will fail. :pr:`23325`
* Add treatment of low confidence, zero estimations as ``UNKNOWN`` during joins, with the ``treat-low-confidence-zero-estimation-as-unknown`` session property :pr:`23047`
* Add confidence based broadcasting, side of join with highest confidence will be on build side.  This can be enabled with the ``confidence_based_broadcast`` session property :pr:`23016`
* Add :doc:`/clients/dbeaver` documentation. :pr:`23189`
* Add :doc:`/clients/superset` documentation. :pr:`23194`
* Upgrade Joda-Time to 2.12.7 to use 2024a tzdata. Note: a corresponding update to the Java runtime should also be made to ensure consistent timezone data. For example, Oracle JDK 8u381, tzdata2024a rpm for OpenJDK, or use Timezone Updater Tool to apply 2024a tzdata to existing JVM. :pr:`23027`
* Upgrade Airlift to 0.215. :pr:`23356`
* Upgrade avro to 1.11.3 due to CVE-2023-39410. :pr:`23142`
* Upgrade guava to 32.1.0-jre due CVE-2023-2976. :pr:`23127`
* Upgrade json-path to 2.9.0 due to CVE-2023-1370. :pr:`23104`

Presto C++ Changes
__________________
* Add a REST API in Presto C++ worker to fetch worker stats in Prometheus Data Format. :pr:`22360`
* Add CTE materialization for Presto C++ workers with the configuration properties ``hive.temporary-table-storage-format`` (``DWRF`` or ``PARQUET`` only) and ``hive.temporary-table-compression-codec`` (``ZSTD`` or ``NONE`` only). :pr:`22780`
* Add support for persisting full memory cache to SSD periodically on Presto C++ worker. This can be enabled by setting ``cache.velox.full-persistence-interval`` to a non-zero value. :pr:`23192`
* Fix queries that contain timestamp with timezone to fail to avoid correctness issues. :pr:`23200`

JDBC Changes
____________
* Fix failure when setting autoCommit from ``false`` to ``true``. :pr:`23453`
* Fix the ``PrestoDatabaseMetaData.getURL`` method to include the ``jdbc:`` prefix in the returned URL :pr:`23397`

History Based Optimizer Changes
_______________________________
* Fix serialization of aggregation node in HBO plan hash to output consistent hash. :pr:`22949`
* Add session property ``enable_verbose_history_based_optimizer_runtime_stats`` to track latency of HBO optimizer. :pr:`23241`
* Add session property ``enforce_history_based_optimizer_register_timeout`` to enforce the maximum time HBO query registration can take. :pr:`23354`
* Add support for query retry when History-Based Optimization can help a failed query, with the ``retry-query-with-history-based-optimization`` session property :pr:`23147`

Hive Connector Changes
______________________
* Fix default Parquet writer version to ``PARQUET_1_0``. Set the hive config ``hive.parquet.writer.version = PARQUET_2_0`` for old behavior. :pr:`23369`
* Fix filtering by info columns ``$file_size`` and ``$file_modified_time``, which were ignored before. :pr:`23411`
* Fix hash calculation for Timestamp column to be hive compatible when writing to a table bucketed by Timestamp.  :pr:`22980`
* Add config ``hive.legacy-timestamp-bucketing`` and session property ``hive.legacy_timestamp_bucketing`` to use the original hash function for Timestamp column, which is not hive compatible. :pr:`22980`
* Add support for setting the max size in bytes for the directory listing cache. This can be set via the new ``hive.file-status-cache.max-retained-size`` configuration property. ``hive.file-status-cache-size`` is now deprecated. :pr:`23176`
* Add support to skip empty files using configuration property ``hive.skip_empty_files``. :pr:`22727`
* Add support for decimal batch reader :pr:`22636`

Iceberg Connector Changes
_________________________
* Fix default Parquet writer version to ``PARQUET_1_0``. Set the hive config ``hive.parquet.writer.version = PARQUET_2_0`` for old behavior. :pr:`23369`
* Add procedure ``remove_orphan_files`` to remove orphan files that are not referenced in any metadata files for Iceberg. :pr:`23267`
* Add table properties ``metadata_previous_versions_max`` and ``metadata_delete_after_commit`` to maintain the previous metadata files. :pr:`23260`
* Add support for Iceberg with hive catalog to delete old metadata files after commit based on the table properties. :pr:`23260`
* Add `configuration properties <https://prestodb.io/docs/current/connector/iceberg.html#glue-catalog>` to tune table metadata refresh timeouts for the Iceberg connector when configured with the Hive or Glue catalog. :pr:`23174`
* Fix Iceberg read failing for Decimal type. :pr:`23305`
* Improve performance of Iceberg and Delta connectors when used with JDBC client. :pr:`22936`

Delta Connector Changes
_______________________
* Add support to read tables up to Delta Protocol Version 3,7 through the use of the new Delta-Kernel api. :pr:`22596`
* Improve performance of Iceberg and Delta connectors when used with JDBC client. :pr:`22936`
* Add new boolean configuration parameter delta.case-sensitive-partitions-enabled to be able to query data with partitioned columns with column names in uppercase. This property is set to true by default. :pr:`22596`

Verifier Changes
________________
* Add ``control.reuse-table`` and ``test.reuse-table`` configuration properties for the Presto Verifier to reuse the output tables of the source query for control and test. :pr:`22965`
* Add verifier config ``--validate-string-as-double`` to control applying floating point validation to the column composed of varchar, if the varchar column is derived from casting floating points. :pr:`23312`

SPI Changes
___________
* Add ``publishQueryProgress`` to ``EventListener`` to publish regular progress of queries in a Presto cluster.  The ``event.query-progress-publish-interval`` config property can be used to specify the time interval at which progress events should be generated. Default is 0 (disabled). :pr:`23195`
* Add ``equalValuesAreIdentical`` to ``Type``.  Override this method to return ``false`` when the values of the type may have more than one representation. :pr:`23319`

**Credits**
===========

Abe Varghese, Abhisek Saikia, Ajay George, Amit Dutta, Andrii Rosa, Anil Gupta Somisetty, Arjun Gupta, Auden Woolfson, Bikramjeet Vig, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Devesh Agrawal, Elliotte Rusty Harold, Emanuel F., Feilong Liu, Gary Helmling, Ge Gao, Jacob Khaliqi, Jalpreet Singh Nanda (:imjalpreet), Jialiang Tan, Jimmy Lu, Karteekmurthys, Ke, Kevin Wilfong, Krishna Pai, Linsong Wang, Mahadevuni Naveen Kumar, Matt Calder, Miguel Blanco Godón, Nikhil Collooru, Pramod Satya, Pratik Joseph Dabre, Ramesh Kanna S, Rebecca Schlussel, Reetika Agrawal, Sergey Pershin, Sreeni Viswanadha, Steve Burnett, Swapnil Tailor, Tim Meehan, Wills Feng, Yihong Wang, Zac Blanco, Zac Wen, Zuyu ZHANG, abhinavmuk04, aditi-pandit, cvarelad-denodo, jaystarshot, misterjpapa, oyeliseiev-ua, prithvip, wangd, wypb, xiaoxmeng, yingsu00, ymmarissa
