=============
Release 0.273
=============

.. warning::

   There is a regression in query execution times due to way we are processing HdfsConfiguration
   and the issue is fixed in 0.273.2 release.

**Highlights**
==============
* Add new Presto connector for Clickhouse.

**Details**
===========

General Changes
_______________
* Fix disaggregated coordinator resource group aggregation issue related to ``WAITING_FOR_PREREQUISITE`` queries. When a query is moved to WAITING_FOR_PREREQUISITE, resource manager would end up counting them towards running queries. This gives a wrong impression to coordinator about running queries and it would end up running less queries than configured.
* Fix a bug where ``EXPLAIN (TYPE IO)`` did not include the index source from an index join.
* Fix a memory over-counting issue for using varchar types that could cause queries to fail with exceeded memory limit errors.
* Fix an off-by-one error during buffer size calculation in :func:`tdigest_agg`.
* Fix memory tagging for UnnestOperator and TableWriterMergeOperator.
* Improve performance of :func: `zip_with` when used inside a :func:`reduce_agg`.
* Add :func:`to_base32` and :func:`from_base32` varbinary functions.
* Add support for ``TRUNCATE TABLE`` statement and associated :doc:`/sql/truncate`.
* Add support to set timeout on the query analyzer runtime to prevent long running query analysis like complex regular expressions. The timeout duration can be set by the configuration property ``planner.query-analyzer-timeout`` or session property ``query_analyzer_timeout``.
* Add :func:`trim_array` function that can be called to delete elements from the end of an ordinary array.
* Add a new optimization for showing results for (interactive) distinct limit N as they become available with no buffering.  This can be enabled by setting the session property ``quick_distinct_limit_enabled`` to true.
* Add support for casting timestamp with micro/nanosecond precision.
* Add Read support for json & jsonb data types in postgresql.
* Add support for local round robin shuffle to reduce the partial distinct limit output size.
* Add support for column subfield access control checks in connectors. Connectors can specify subfield access control for row-type columns through ``checkCanSelectFromColumns()`` in SPI.
* Add warnings for :func:`approx_distinct` and :func:`approx_set` with low ``MAX_STANDARD_ERROR``.
* Add support for retrying queries that failed with max requests queued exception.
* Update protoc and grpc version in presto-grpc-api module to adapt to linux aarch64.
* Upgrade zstandard compression to version 1.5.2.2. This improves the cpu used for data compressed with zstandard by about 2%.
* Upgrade postgresql driver to 42.3.3.

Clickhouse Changes
____________
* Add a Presto connector for Clickhouse with support of username/password authentication.

Hive Changes
____________
* Add a limit that prevents caching partitions with column counts greater than a configured threshold. This threshold can be set using the ``hive.partition-cache-column-count-limit`` configuration property.
* Add metastore configuration property ``hive.metastore.thrift.delete-files-on-table-drop`` to delete files on drop table.
* Add support for overwriting existing partitions with a Hive configuration property ``hive.insert-existing-partitions-behavior``. This configuration property supersedes the legacy one ``hive.insert-overwrite-immutable-partitions-enabled``. The new configuration property adds capability of overwriting new partitions for S3.
* Add support for timely Parquet metadata cache invalidation.
* Replace Hive session property ``streaming_aggregation_enabled`` with ``order_based_execution_enabled``.
* Replace Hive configuration property ``hive.streaming-aggregation-enabled`` with ``hive.order-based-execution-enabled``.

Iceberg Changes
_______________
* Fix Iceberg ``$files`` table in case of column dropping.
* Add ``$properties`` system table.
* Add support for storing column comments for Iceberg tables.
* Upgrade Iceberg to 0.13.1.

Mongodb Changes
_______________
* Add :func:`CAST(ObjectId() as STRING)`.

Pinot Changes
_____________
* Improve query performance by enabling pushdown of topn broker queries by default.
* Add double-quotes to Pinot generated queries to ensure that reserved keywords are escaped.
* Add TLS support in Pinot gRPC connection.
* Upgrade Pinot connector to support Pinot 0.10.0.

Spark Changes
_____________
* Improve handling of scenario where a query fails due to broadcast table violating node memory limit or broadcast limit. Previously the query failed while workers tried to load broadcast table causing container out of memory.
* Add a new configuration property ``spark.retry-on-out-of-memory-broadcast-join-enabled`` to disable broadcast join on broadcast OOM and retry the query again within the same spark session.  This can be overridden by ``spark_retry_on_out_of_memory_broadcast_join_enabled`` session property.

Verifier Changes
________________
* Add support to verifier for running control and test queries concurrently by setting ``concurrent-control-and-test`` configuration property to true.

SPI Changes
___________
* Add ``getInfoMap`` method in ``ConnectorSplit`` which returns a ``Map<String, String>``. This method should be preferred to the ``getInfo`` method which returns a raw object.

**Credits**
===========

Ajay George, Amit Adhikari, Amit Dutta, Anuj Jamwal, Ariel Weisberg, Arjun Gupta, Arunachalam Thirupathi, Aryan, Asjad Syed, Beinan, Boris Verkhovskiy, Branimir Vujicic, Chen Li, Chunxu Tang, Derek Xia, Eduard Tudenhoefner, Fengpeng Yuan, Guy Moore, Harsha Rastogi, James Petty, James Sun, Jaromir Vanek, Jun, Ke Wang, Lin Liu, Maksim Dmitriyevich Podkorytov, Mandy Cho, Maria Basmanova, Michael Shang, Naveen Kumar Mahadevuni, Nikhil Collooru, Nirmit Shah, Ohm Patel, Pranjal Shankhdhar, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Rongrong Zhong, Ruslan Mardugalliamov, Sergey Pershin, Sergii Druzkin, Shashwat Arghode, Sreeni Viswanadha, Steve Chuck, Swapnil Tailor, Tim Meehan, Todd Gao, Valentin Touffet, Will Holen, Xiang Fu, Xiaoman Dong, Xinli shang, Yeikel, Zac, Zhenxiao Luo, abhiseksaikia, ellison840611, imjalpreet, shidayang, singcha, v-jizhang, xingmao.zheng, xyueji, yangyicheng, zhangbutao
