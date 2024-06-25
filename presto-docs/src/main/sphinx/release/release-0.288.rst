=============
Release 0.288
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix CAST of REAL values outside of BIGINT range to return an exception with an INVALID_CAST_ARGUMENT error code. Previously they would silently overflow.
* Fix HBO to skip tracking of stats for plan nodes affected by dynamic filter pushdown in presto cpp :pr:`22853 `.
* Fix a bug where map_top_n could return wrong results if there is any NaN input.
* Fix a bug with array_min/array_max where it would return NaN rather than null when there was both NaN and null input.
* Fix an error for some queries using a mix of joins and semi-joins when grouped execution is enabled.
* Fix array_join to not add a trailing delimeter when the last element in the array is NULL. :pr:`22652`.
* Fix cast of NaN and Infinity from DOUBLE or REAL to  BIGINT, INTEGER, SMALLINT, and TINYINT. It will now return an exception with the INVALID_CAST_ARGUMENT error code. Previously it would return zero.
* Fix compilation error for queries with lambda in aggregation function.
* Fix incorrect behaviors when defining duplicate field names in RowType and throw exception uniformly.
* Fix regr_r2 result incorrect.
* Fix the latency regression for queries with large IN clause.
* Fix wrong results when queries using materialized CTEs have multiple common filters pushed into the CTE :pr:`22700 `.
* Improve :doc:`/sql/explain-analyze` statement to support a ``format`` argument with values of ``<TEXT|JSON>`` :pr:`22733`.
* Improve README.md and CONTRIBUTING.md :pr:`22918`.
* Improve configuring worker threads relative to core count by setting the ``task.max-worker-threads`` configuration property to ``<multiplier>C``. For example, setting the property to ``2C`` configures the worker thread pool to create up to twice as many threads as there are cores available on a machine. :pr:`22809`.
* Improve logging for RowExpressionRewriteRuleSet and StatsRecordingPlanOptimizer optimizers to include more information :pr:`22765`.
* Improve session property ``property-use_broadcast_when_buildsize_small_probeside_unknown`` to do broadcast join when probe side size is unknown and build side estimation from HBO is small.
* Improve the estimation stats recorded during query optimization :pr:`22769 `.
* Add :doc:`/presto_cpp/properties` documentation :pr:`22885`.
* Add PR number to the release note entry examples in pull_request_template.md :pr:`22665`.
* Add Prestissimo Properties Reference page to the Prestissimo Developer Guide documentation :pr:`22620`.
* Add `http-server.authentication.allow-forwarded-https` configuration property to recognize X-Forwarded-Proto header, :pr:`22492`.
* Add `node-scheduler.max-preferred-nodes` configuration property to allow changing number of preferred nodes when soft affinity scheduling is enabled. :pr:`22562`.
* Add documentation for :func:`noisy_approx_set_sfm_from_index_and_zeros`.
* Add documentation for noisy aggregate functions at :doc:`/functions/noisy`, including :func:`noisy_approx_distinct_sfm` and :func:`noisy_approx_set_sfm` (:pr:`21290`, :pr:`22715`).
* Add support for memoizing in resource group state info endpoint. This can be enabled by setting `cluster-resource-group-state-info-expiration-duration` to a non-zero duration. :pr:`22764`.
* Add support for non default keystore and truststore type.
* Add support for querying system.runtime.tasks table in native clusters.
* Remove deprecated feature and configuration property ``deprecated.group-by-uses-equal``, which allowed group by to use equal to rather than distinct semantics.
* ... Improve C++ based Presto documentation :pr:`22717`.
* Change error code for cast from DOUBLE or REAL to BIGINT, INTEGER, SMALLINT or TINYINT for out of range values from ``NUMERIC_VALUE_OUT_OF_RANGE`` to ``INVALID_CAST_ARGUMENT``.
* Change handling of floating point numbers in Presto to consistently treat NaNs as larger than any other number and equal to itself.  It also changes the handling of positive and negative zero to always be considered equal to each other.  Read more here: https://github.com/prestodb/rfcs/blob/main/RFC-0001-nan-definition.md. The new nan behavior can be disabled by setting the configuration property ``use-new-nan-definition`` to ``false``. This configuration property is intended to be temporary to ease migration in the short term, and will be removed in a future release.
* Enable HBO for CTE materialized query :pr:`22606`.
* Prestissimo support for CTAS into bucketed (but not partitioned) tables pr:`22737`.
* Update CI pipeline to build and publish native worker docker image :pr:`22806`.
* Upgrade Alluxio to 313.
* Upgrade io.jsonwebtoken artifacts to 0.11.5 :pr:`22762`.
* Upgrades fasterxml.jackson artifacts to 2.11 :pr:`22417`.

Hive Connector Changes
______________________
* Improve affinity scheduling granularity from a file to a section of a file by adding a `hive.affinity-scheduling-file-section-size` configuration property and `affinity_scheduling_file_section_size` session property. The default file size is 256MB. :pr:`22563`.

Iceberg Connector Changes
_________________________
* Improve the partition specs that must be checked to determine if the partition supports metadata deletion or predicate thoroughly pushdown :pr:`22753`.
* Improve time travel ``TIMESTAMP (SYSTEM_TIME)`` syntax to include timestamp-with-time-zone data type :pr:`22851`.
* Improve time travel ``VERSION (SYSTEM_VERSION)`` syntax to include snapshot id using bigint data type :pr:`22851`.
* Add procedure `expire_snapshots` to remove old snapshots in Iceberg. :pr:`22609`.
* Add support for Iceberg REST catalog :pr:`22417`.
* Add time travel ``BEFORE`` syntax for Iceberg tables to return historical data :pr:`22851`.
* Disable timestamp with time zone in create, alter and insert statements :pr:`22926`.

Verifier Changes
________________
* Support function call substitution based on the specified substitution pattern passed by the parameter --function-substitutes.

SPI Changes
___________
* Add runtime stats as parameter to `ConnectorPageSourceProvider`.

Hive Changes
____________
* Introduce AWS Security Mapping which will allow flexible mapping of Presto Users to AWS Credentials or IAM Roles for different AWS Services.

Iceberg Changes
_______________
* Support metadata delete with predicate on non-identity partition columns when they align with partitioning boundaries.

**Credits**
===========

8dukongjian, Abhisek Saikia, Ajay Gupte, Amit Dutta, Andrii Rosa, Beinan Wang, Christian Zentgraf, Deepak Majeti, Denodo Research Labs, Elliotte Rusty Harold, Emanuel F, Emanuel F., Fazal Majid, Feilong Liu, Ge Gao, Jalpreet Singh Nanda (:imjalpreet), Jialiang Tan, Jimmy Lu, Jonathan Hehir, Karteekmurthys, Ke, Kevin Wilfong, Konjac Huang, Linsong Wang, Michael Shang, Neerad Somanchi, Nidhin Varghese, Nikhil Collooru, Pranjal Shankhdhar, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Sean Yeh, Sergey Pershin, Sergii Druzkin, Sreeni Viswanadha, Steve Burnett, Swapnil Tailor, Tishyaa Chaudhry, Vivek, Vivian Hsu, Wills Feng, Yedidya Feldblum, Yihao Zhou, Yihong Wang, Ying, Zac Blanco, Zac Wen, abhinavmuk04, aditi-pandit, deepthydavis, jackychen718, jaystarshot, kiersten-stokes, wangd, wypb, xiaoxmeng, ymmarissa
