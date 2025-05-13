=============
Release 0.288
=============

.. warning::

   The tarball package ``presto-server-0.288.tar.gz`` does not include the Presto Console.  This has been fixed in the patch version ``0.288.1``. :issue:`23327`

**Highlights**
==============
* Improve handling of floating point numbers in Presto to consistently treat NaNs as larger than any other number and equal to itself. It also changes the handling of positive and negative zero to always be considered equal to each other. For more information, see `RFC-0001-nan-definition.md <https://github.com/prestodb/rfcs/blob/main/RFC-0001-nan-definition.md>`_. The new nan behavior can be disabled by setting the configuration property use-new-nan-definition to false. This configuration property is intended to be temporary to ease migration in the short term, and will be removed in a future release. :pr:`22386`
* Add procedure `expire_snapshots` to remove old snapshots in Iceberg. :pr:`22609`
* Add support for Iceberg REST catalog. :pr:`22417`
* Add support for ``NOT NULL`` column constraints in the CREATE TABLE and ALTER TABLE statements. This only takes effect for Hive connector now. :pr:`22064`

**Details**
===========

General Changes
_______________
* Fix CAST of REAL values outside of BIGINT range to return an exception with an INVALID_CAST_ARGUMENT error code. Previously they would silently overflow. :pr:`22917`
* Fix HBO to skip tracking of stats for plan nodes affected by dynamic filter pushdown in presto cpp. :pr:`22853`
* Fix a bug where :func:`!map_top_n` could return wrong results if there is any NaN input. :pr:`22386`
* Fix a bug with array_min/array_max where it would return NaN rather than null when there was both NaN and null input. :pr:`22386`
* Fix an error for some queries using a mix of joins and semi-joins when grouped execution is enabled. :pr:`22538`
* Fix :func:`!array_join` to not add a trailing delimeter when the last element in the array is NULL. :pr:`22652`
* Fix cast of NaN and Infinity from DOUBLE or REAL to  BIGINT, INTEGER, SMALLINT, and TINYINT. It will now return an exception with the INVALID_CAST_ARGUMENT error code. Previously it would return zero. :pr:`22917`
* Fix compilation error for queries with lambda in aggregation function. :pr:`22539`
* Fix incorrect behaviors when defining duplicate field names in RowType and throw exception uniformly. :pr:`22618`
* Fix wrong results for :func:`!regr_r2`. :pr:`22611`
* Fix the latency regression for queries with large IN clause. :pr:`22661`
* Fix wrong results when queries using materialized CTEs have multiple common filters pushed into the CTE. :pr:`22700`
* Improve :doc:`/sql/explain-analyze` statement to support a ``format`` argument with values of ``<TEXT|JSON>``. :pr:`22733`
* Improve README.md and CONTRIBUTING.md. :pr:`22918`
* Improve configuring worker threads relative to core count by setting the ``task.max-worker-threads`` configuration property to ``<multiplier>C``. For example, setting the property to ``2C`` configures the worker thread pool to create up to twice as many threads as there are cores available on a machine. :pr:`22809`
* Improve logging for RowExpressionRewriteRuleSet and StatsRecordingPlanOptimizer optimizers to include more information. :pr:`22765`
* Improve session property ``property-use_broadcast_when_buildsize_small_probeside_unknown`` to do broadcast join when probe side size is unknown and build side estimation from HBO is small. :pr:`22681`
* Improve the estimation stats recorded during query optimization. :pr:`22769`
* Improve Presto C++ documentation. :pr:`22717`
* Improve error code for cast from DOUBLE or REAL to BIGINT, INTEGER, SMALLINT or TINYINT for out of range values from NUMERIC_VALUE_OUT_OF_RANGE to INVALID_CAST_ARGUMENT. :pr:`22917`
* Improve handling of floating point numbers in Presto to consistently treat NaNs as larger than any other number and equal to itself. It also changes the handling of positive and negative zero to always be considered equal to each other. Read more here: https://github.com/prestodb/rfcs/blob/main/RFC-0001-nan-definition.md. The new nan behavior can be disabled by setting the configuration property use-new-nan-definition to false. This configuration property is intended to be temporary to ease migration in the short term, and will be removed in a future release. :pr:`22386`
* Improve the performance of reading common table expressions (CTE). :pr:`22478`
* Improve join performance by prefiltering the build side with distinct keys from the probe side. This can be enabled with the ``join_prefilter_build_side`` session property. :pr:`22667`
* Add HBO for CTE materialized query. :pr:`22606`
* Add support for CTAS on bucketed (but not partitioned) tables for Presto C++ clusters. :pr:`22737`
* Add support for ``NOT NULL`` column constraints in the CREATE TABLE and ALTER TABLE statements. This only takes effect for Hive connector now. :pr:`22064`
* Add :doc:`/presto_cpp/properties` documentation. :pr:`22885`
* Add PR number to the release note entry examples in pull_request_template.md. :pr:`22665`
* Add ``http-server.authentication.allow-forwarded-https`` configuration property to recognize X-Forwarded-Proto header. :pr:`22492`
* Add ``node-scheduler.max-preferred-nodes`` configuration property to allow changing number of preferred nodes when soft affinity scheduling is enabled. :pr:`22562`
* Add documentation for :func:`!noisy_approx_set_sfm_from_index_and_zeros`. :pr:`22799`
* Add documentation for noisy aggregate functions at :doc:`/functions/noisy`, including :func:`!noisy_approx_distinct_sfm` and :func:`!noisy_approx_set_sfm`. :pr:`22715`
* Add support for memoizing in resource group state info endpoint. This can be enabled by setting ``cluster-resource-group-state-info-expiration-duration`` to a non-zero duration. :pr:`22764`
* Add support for non default keystore and truststore type in presto CLI and JDBC. :pr:`22556`
* Add support for querying system.runtime.tasks table in Presto C++ clusters. :pr:`21416`
* Add two system configuration properties to specify the reserved query memory capacity on Presto C++ clusters: ``query-reserved-memory-gb`` is the total amount of memory in GB reserved for the queries on a worker node. ``memory-pool-reserved-capacity`` is the amount of memory in bytes reserved for each query. :pr:`22593`
* Add a configuration option ``cache.last-modified-time-check-enabled`` to enable last modified time checks for cached files in Alluxio to ensure they are up-to-date. :pr:`22750`
* Replace the Presto native stats definition and reporting for the memory allocator, in-memory cache and ssd cache metrics from Presto repo to Velox repo, with the metrics names changing from presto_cpp.<metrics_name> to velox.<metrics_name>. :pr:`22751`
* Remove deprecated feature and configuration property ``deprecated.group-by-uses-equal``, which allowed group by to use equal to rather than distinct semantics. :pr:`22888`
* Upgrade CI pipeline to build and publish Presto C++ worker docker image. :pr:`22806`
* Upgrade Alluxio to 313. :pr:`22958`
* Upgrade io.jsonwebtoken artifacts to 0.11.5. :pr:`22762`
* Upgrade fasterxml.jackson artifacts to 2.11. :pr:`22417`

Hive Connector Changes
______________________
* Fix hash calculation for Timestamp column to be hive compatible when writing to a table bucketed by Timestamp. :pr:`22980`
* Improve affinity scheduling granularity from a file to a section of a file by adding a ``hive.affinity-scheduling-file-section-size`` configuration property and ``affinity_scheduling_file_section_size`` session property. The default file size is 256MB. :pr:`22563`
* Add AWS Security Mapping to allow flexible mapping of Presto Users to AWS Credentials or IAM Roles for different AWS Services. :pr:`21622`
* Add config property ``hive.legacy-timestamp-bucketing`` and session property ``hive.legacy_timestamp_bucketing`` to use the original hash function for Timestamp column, which is not hive compatible. :pr:`22980`
* Add support for ``NOT NULL`` column constraints in the CREATE TABLE and ALTER TABLE statements for the Hive connector. :pr:`22064`

Iceberg Connector Changes
_________________________
* Improve the partition specs that must be checked to determine if the partition supports metadata deletion or predicate thoroughly pushdown. :pr:`22753`
* Improve time travel ``TIMESTAMP (SYSTEM_TIME)`` syntax to include timestamp-with-time-zone data type. :pr:`22851`
* Improve time travel ``VERSION (SYSTEM_VERSION)`` syntax to include snapshot id using `BIGINT` data type. :pr:`22851`
* Add procedure `expire_snapshots` to remove old snapshots in Iceberg. :pr:`22609`
* Add support for Iceberg REST catalog. :pr:`22417`
* Add time travel ``BEFORE`` syntax for Iceberg tables to return historical data. :pr:`22851`
* Add support for metadata delete with predicate on non-identity partition columns when they align with partitioning boundaries. :pr:`22554`
* Remove timestamp with time zone in ``CREATE``, ``ALTER``, and ``INSERT`` statements. :pr:`22926`
* Add configuration of Iceberg split manager threads using the iceberg.split-manager-threads configuration property. :pr:`22754`

Verifier Changes
________________
* Add support for function call substitution based on the specified substitution pattern passed by the parameter --function-substitutes. :pr:`22783`

SPI Changes
___________
* Add runtime stats as parameter to ``ConnectorPageSourceProvider``. :pr:`22960`

**Credits**
===========

8dukongjian, Abhisek Saikia, Ajay Gupte, Amit Dutta, Andrii Rosa, Beinan Wang, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Elliotte Rusty Harold, Emanuel F, Fazal Majid, Feilong Liu, Ge Gao, Jalpreet Singh Nanda (:imjalpreet), Jialiang Tan, Jimmy Lu, Jonathan Hehir, Karteekmurthys, Ke, Kevin Wilfong, Konjac Huang, Linsong Wang, Michael Shang, Neerad Somanchi, Nidhin Varghese, Nikhil Collooru, Pranjal Shankhdhar, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Sean Yeh, Sergey Pershin, Sergii Druzkin, Sreeni Viswanadha, Steve Burnett, Swapnil Tailor, Tishyaa Chaudhry, Vivek, Vivian Hsu, Wills Feng, Yedidya Feldblum, Yihao Zhou, Yihong Wang, Ying Su, Zac Blanco, Zac Wen, abhinavmuk04, aditi-pandit, deepthydavis, jackychen718, jaystarshot, kiersten-stokes, wangd, wypb, xiaoxmeng, ymmarissa
