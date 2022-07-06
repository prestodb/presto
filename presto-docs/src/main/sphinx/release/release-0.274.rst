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
* Add :func:`laplace_cdf` and :func:`inverse_laplace_cdf` functions.
* Reduce the memory footprint and improves the performance of segmented aggregation when the data is already ordered by a subset of the group-by keys. This can be enabled with the ``segmented_aggregation_enabled`` session property or the ``optimizer.segmented-aggregation-enabled`` configuration property.
* Add memory limit check for join spilling and fail fast if exceeding to avoid unnecessary processing.
* Add rate limiting functionality for each query on coordinator http endpoints.
* Add support for ``LIKE`` predicate to ``SHOW SESSION`` and ``SHOW CATALOGS`` with ``ESCAPE`` support.
* Add support for constraint optimizations to improve performance for querying catalogs that supports table constraints. This feature can be turned on by setting the session property ``exploit-constraints`` or config property ``optimizer.exploit-constraints`` to true.
* Add support for local mode on ``/v1/cluster/`` to get information on locally running/queued queries. Can be enabled with the ``includeLocalInfoOnly`` property.
* When cost-based optimizer fails, consider source table size when choosing join order and join distribution type. This might improve query plan for queries with complex projections on small tables. This optimization can be disabled by setting session property ``size_based_join_distribution_type`` to ``false``.

SPI Changes
___________
* Add connector interface serde support to allow customized serde (e.g., thrift) beyond the default JSON ones.

Hive Changes
____________
* Add support for segmented aggregation to reduce the memory footprint and improve query performance when the order-by keys are a subset of the group-by keys. This can be enabled with the ``order_based_execution_enabled`` session property or the ``hive.order-based-execution-enabled`` configuration property.
* Upgrade Hudi support to 0.11.0.
* Use ``REQUIRED`` repetition level for MAP keys in parquet writer.
* Add decryption functionality to Parquet. When a Parquet file is encrypted following `Parquet Modular Encryption <https://github.com/apache/parquet-format/blob/master/Encryption.md>`_, this change enables Presto to be able to decrypt.

Iceberg Changes
_______________
* Add ``iceberg.max-partitions-per-writer`` configuration property to allow configuring the limit on partitions per writer.
* Add support for Nessie catalog.
* Upgrade Iceberg support to 0.13.2

Mongodb Changes
_______________
* Support renaming tables.  Users can use ``ALTER TABLE RENAME TO`` syntax to rename a table.

Pinot Changes
_____________
* Fix query error when applying aggregation function over timestamp columns (:issue:`17755`).
* Add support for ``CASE`` statement pushdown to Pinot.
* Add support for logical and arithmetic functions for ``CASE`` statements.
* Prefer querying Pinot server grpc endpoint by default (through config ``pinot.use-streaming-for-segment-queries``).

**Credits**
===========

Ajay George, Amit Adhikari, Amit Dutta, Andy Li, Arjun Gupta, Arunachalam Thirupathi, Beinan, Chunxu Tang, David Simmen, Dongsheng Wang, Eduard Tudenhoefner, Eric Kwok, Guy Moore, Harleen Kaur, Harsha Rastogi, James Sun, Jaromir Vanek, Jonathan Hehir, Ke Wang, Lin Liu, Maria Basmanova, Michael Shang, Nikhil Collooru, Patrick Sullivan, Pranjal Shankhdhar, Rebecca Schlussel, Reetika Agrawal, Rongrong Zhong, Sergey Pershin, Sergii Druzkin, Swapnil Tailor, Tim Meehan, Todd Gao, Xiang Fu, Xiaoman Dong, Xinli shang, Zac, Zhenxiao Luo, abhiseksaikia, ericyuliu, junyi1313, maswin, prithvip, v-jizhang, vaishnavibatni, vigneshwarselvaraj, xiaoxmeng, zhangyanbing
