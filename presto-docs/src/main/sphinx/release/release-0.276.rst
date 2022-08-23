=============
Release 0.276
=============

**Details**
===========

General Changes
_______________
* Fix a bug where a query with an order by clause and an unpartitioned window function sometimes returned unordered results.
* Improve Graphviz output for JOIN nodes and estimate stats.
* Reduce the number of hdfsConfiguration copies in the worker. This feature is enabled by default and can be controlled by setting the ``hive.copy-on-first-write-configuration`` configuration property appropriately.
* Add support for complex JsonPath expressions in :func:`json_extract`, :func:`json_extract_scalar` and :func:`json_size` using `Jayway JsonPath <https://github.com/json-path/JsonPath>`_.
* Add support to nested SQL functions with lambdas when ``inline_sql_functions = false``.
* Add the GeoPlugin by default. Previously it was an optional plugin.
* Add documentation for :doc:`/functions/tdigest`.
* Add ConnectorPlanRewriter utility to simplify writing Connector specific optimizer rules.
* Make ``query.max-total-memory-per-node property`` optional with default value dependent on ``query.max-memory-per-node``.

Hive Changes
____________
* Add support of function registration for interface `org.apache.hadoop.hive.ql.exec.UDF``.

Hudi Changes
____________
* Add sized-based split weights for Hudi connector to improve query performance. This is enabled by default, controlled by configuration property ``hudi.size-based-split-weights-enabled`` and session property ``hudi.size_based_split_weights_enabled``. Two more configuration properties are added to adjust the weight: ``hudi.standard-split-weight-size`` to configure the split size corresponding to the standard split weight, and ``hudi.minimum-assigned-split-weight`` to configure the minimum split weight.

Iceberg Changes
_______________
* Update Iceberg to 0.14.0.

Pinot Changes
_____________
* Fix an incorrect result issue caused by cross-join query pushdown by throwing errors instead of providing the wrong answer.
* Add new config ``pinot.attempt-broker-queries`` to attempt to pushdown queries to brokers.
* Add support for pinot controller and broker authentication with user and password.
* Add pushdown support for :func:`coalesce` function.

Router Changes
______________
* Add a round-robin scheduler.
* Add a weighted random choice scheduler.
* Add an optional config for the query predictor uri.
* Add presto router's doc.

Spark Changes
_____________
* Add a new configuration property ``spark.retry-on-out-of-memory-with-increased-memory-settings-enabled`` to enable picking up presto and spark memory settings and retry the query within the same spark session with the new settings applied.  This can be overridden by ``spark_retry_on_out_of_memory_with_increased_memory_settings_enabled`` session property.
* Add new configuration properties ``spark.retry-presto-session-properties`` and ``spark.retry-spark-configs`` to alter the Presto session properties and Spark settings during retry respectively. They can be overridden by ``out_of_memory_retry_presto_session_properties`` and ``out_of_memory_retry_spark_configs`` session properties.
* Add a new configuration property ``spark.resource-allocation-strategy-enabled`` and its session property ``spark_resource_allocation_strategy_enabled`` to allow optimized resource allocation strategy. This enables automatic executor and hash partition count to be estimated during planning time. The estimation could be bounded by configurations including ``spark.average-input-datasize-per-executor``, ``spark.max-executor-count``, ``spark.min-executor-count``, ``spark.average-input-datasize-per-partition``, ``spark.max-hash-partition-count``, and ``spark.min-hash-partition-count``. There are also corresponding session properties for these estimations.


**Credits**
===========

Amit Pandey, Anant Aneja, Andy Li, Arjun Gupta, Arunachalam Thirupathi, Beinan Wang, Chunxu Tang, Daniel Izcovich, Eduard Tudenhoefner, Feilong Liu, George Wang, Harsh Kevadia, James Sun, James Turner, Maria Basmanova, Michael Shang, Nikhil Collooru, Onder Kaya, Patrick Sullivan, Pranjal Shankhdhar, Rebecca Schlussel, Reetika Agrawal, Rongrong Zhong, Sacha Viscaino, Sergey Pershin, Sergii Druzkin, Sourav Pal, Swapnil Tailor, Timothy Meehan, Todd Gao, Vivek, Xiang Fu, Y Ethan Guo, abhiseksaikia, dnskr, ericyuliu, pratyakshsharma, shidayang, v-jizhang
