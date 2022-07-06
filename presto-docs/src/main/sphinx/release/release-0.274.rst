=============
Release 0.274
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix broadcast join memory leak caused by exceeding local memory query failure.
* Fix infinite loop during planning caused by PickTableLayout.
* Fix the UI displaying name and systemMemoryContext allocationTag of OptimizedPartitionedOutputOperator.
* Add :func:`laplace_cdf` and :func:`inverse_laplace_cdf` functions.
* Add ability to flush the aggregated data when at lease one segment from the input has been exhausted. It can help reduce the memory footprint and improves the performance of aggregation when the data is already ordered by a subset of the group-by keys. This can be enabled with the ``segmented_aggregation_enabled`` session property or the ``optimizer.segmented-aggregation-enabled`` configuration property.
* Add connector type serde support.
* Add decryption functionality to Presto. When a Parquet file is encrypted following [Parquet Modular Encryption](https://github.com/apache/parquet-format/blob/master/Encryption.md), this change enables Presto to be able to decrypt.
* Add memory limit check for HashBuilderOperator's spilling and fail fast if exceeding to avoid unnecessary processing.
* Add rate limiting functionality for each query on coordinator http endpoints.
* Add support for ``LIKE`` predicate to ``SHOW SESSION``.
* Add support for constraint optimizations in Hive MetaStore. This feature can be turned on by setting the session property "exploit-constraints" and config property "optimizer.exploit-constraints" to true.
* Add support for local mode on ``/v1/cluster/`` to get information on locally running/queued queries. Can be enabled with the ``includeLocalInfoOnly`` property.
* Add the support for setting units of runtime metrics.
* When CBO fails, consider source table size when choosing join order and join distribution type. This might improve query plan for queries with complex projections on small tables. This optimization can be disabled by setting session property ``size_based_join_distribution_type`` to ``false``.

Web UI Changes
______________
* Add clickhouse connector document in connector list.

SPI Changes
___________
* Add connector type serde support in SPI.

Hive Changes
____________
* Add support for segmented aggregation to reduce the memory footprint and improve query performance when the order-by keys are a subset of the group-by keys. This can be enabled with the ``order_based_execution_enabled`` session property or the ``hive.order-based-execution-enabled`` configuration property.
* Handle missing StorageDescriptor in Hive Glue tables.
* No flag is introduced. Presto-Hive was changed by adding the loading DecryptionPropertiesFactory(implemented in parquet-mr) and using it to get the file decryptor and pass it to presto-parquet.
* Upgrade Hudi support to 0.11.0.
* Use REQUIRED repetition level for MAP keys in parquet writer.

Iceberg Changes
_______________
* Add `iceberg.max-partitions-per-writer` configuration property to allow configuring the limit on partitions per writer.
* Add support for NessieCatalog.

Mongodb Changes
_______________
* Support renaming tables.  Users can use ``ALTER TABLE RENAME TO`` to rename a table.

Pinot Changes
_____________
* Fix query error when applying aggregation function over timestamp columns (:issue:`17755`).
* Add support for `CASE` statement pushdown to Pinot.
* Add support for logical and arithmetic functions for `CASE` statements.
* Prefer querying Pinot server grpc endpoint by default (through config `pinot.use-streaming-for-segment-queries`).

**Credits**
===========

Ajay George, Amit Adhikari, Amit Dutta, Andy Li, Arjun Gupta, Arunachalam Thirupathi, Beinan, Chunxu Tang, David Simmen, Dongsheng Wang, Eduard Tudenhoefner, Eric Kwok, Guy Moore, Harleen Kaur, Harsha Rastogi, James Sun, Jaromir Vanek, Jonathan Hehir, Ke Wang, Lin Liu, Maria Basmanova, Michael Shang, Nikhil Collooru, Patrick Sullivan, Pranjal Shankhdhar, Rebecca Schlussel, Reetika Agrawal, Rongrong Zhong, Sergey Pershin, Sergii Druzkin, Swapnil Tailor, Tim Meehan, Todd Gao, Xiang Fu, Xiaoman Dong, Xinli shang, Zac, Zhenxiao Luo, abhiseksaikia, ericyuliu, junyi1313, maswin, prithvip, v-jizhang, vaishnavibatni, vigneshwarselvaraj, xiaoxmeng, zhangyanbing
