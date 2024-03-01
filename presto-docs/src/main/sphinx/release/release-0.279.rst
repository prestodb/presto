=============
Release 0.279
=============

**Details**
===========

General Changes
_______________
* Fix a bug in ``CombineApproxPercentileFunctions`` optimization that caused query failures when there were duplicate aggregation expressions.
* Fix disaggregated coordinator task limit enforcement.
* Fix weighted fair scheduling in disaggregated coordinator.
* Fix max(x, n) and min(x, n) returning NULL on window aggregations.
* Add an optimization that removes redundant distinct if the output is already distinct after a group by operation. The optimization is controlled by session property ``remove_redundant_distinct_aggregation`` which is default to false.
* Add function :func:`array_sort_desc` to sort an array in the descending order.
* Add function :func:`map_remove_null_values` to remove all the entries where the value is null from a given map.
* Add function :func:`map_top_n_keys` to returns an array of the top N keys of the provided map. An optional lambda comparator can also be passed to perform a custom comparison of the keys. Returns all the keys if the value N is greater than or equal to size of the map. For N < 0, the function returns keys in the reverse order. For N = 0, the function returns empty array.
* Add function :func:`map_top_n_values` to return top N values of the provided map. An optional lambda comparator can also be passed as parameter for custom sorting of the values.
* Add function :func:`map_top_n` to truncate map items, keeping only the top N elements by value.
* Add function :func:`remove_nulls` to remove null elements from an array.
* Add functions :func:`array_min_by`, :func:`array_max_by`, to find the smallest or largest element of an array when applying a custom measuring function.
* Extend functions :func:`array_frequency`, :func:`array_duplicates`, :func:`array_has_duplicates`, :func:`array_intersect(array(array(E))` to accept any type as input instead of only varchar/double.
* Add ``CONTROL`` as a new ``QueryType``. The ``CONTROL`` queryType represents statements of session control and transaction control types.
* Remove two unused session parameters - ``hash_based_distinct_limit_enabled`` and ``hash_based_distinct_limit_threshold`` and the corresponding implementation in favor of the ``quick_distinct_limit_enabled`` feature.

Elasticsearch Connector Changes
_______________________________
* Add support for Elasticsearch user and password authentication. :issue:`15909`.

Hive Connector Changes
______________________
* Fix a bug when ``optimize_metadata_queries`` is set to true where queries with aggregations on partition columns and filters on row subfields could return wrong results.
* Disable S3 Select pushdown when the query does not have a predicate or projection.

Iceberg Connector Changes
_________________________
* Update Iceberg from 0.14.1 to 1.0.0.

Pinot Connector Changes
_______________________
* Add pushdown support for function :func:`STRPOS`.

PostgreSQL Connector Changes
____________________________
* Add support for PostgreSQL UUID Data type.

Delta Lake Changes
__________________
* Upgrade Delta Standalone to 0.6.0.

Spark Changes
__________________
* Add property ``spark_executor_allocation_strategy_enabled`` to auto-tune spark max executor count (``spark.dynamicAllocation.maxExecutors``)  based on input data. Only required if ``spark_resource_allocation_strategy_enabled`` is not already enabled.
* Add property ``spark_hash_partition_count_allocation_strategy_enabled`` to auto-tune hash partition count (``hash_partition_count``) based on input data. Only required if ``spark_SPI_allocation_strategy_enabled`` is not already enabled.

Open Telemetry Changes
______________________
* Introduce a new Open Telemetry tracer implementation. The legacy tracer module can be replaced by the new plugin for loading customized tracing infrastructure. Users are able to enable the tracer by installing the ``presto-open-telemetry`` plugin and updating the application configuration (``config.properties``). Open Telemetry tracer can take in propagated context (only B3 specification currently supported) and baggage (W3C specification) headers, if provided, and inject into new traces / spans. Traces can be exported to any specified backend with the ``OTEL_EXPORTER_OTLP_ENDPOINT`` environment variable.

SPI Changes
___________
* Rename ``ConnectorMaterializedViewDefinition`` to  ``MaterializedViewDefinition``.

**Credits**
===========

Aditi Pandit, Alex Chen, Amit Dutta, Anant Aneja, Arjun Gupta, Arunachalam Thirupathi, Asjad Syed, Avinash Jain, Beinan, Christopher Graves, Deepak Majeti, Devesh Agrawal, Eduard Tudenhoefner, Feilong Liu, Ge Gao, George Wang, Guy Moore, Hope Wang, James Petty, James Sun, Jaromir Vanek, Jingmei Huang, Jon Janzen, Josh Soref, JoshuaTang, Karteek Murthy Samba Murthy, Krishna Pai, Linkiewicz, Milosz, Linsong Wang, Lyublena Antova, MJ Deng, Masha Basmanova, Michael Shang, Nizar Hejazi, Pramod, Pranjal Shankhdhar, Pratyaksh Sharma, Pratyush Verma, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Sacha Viscaino, Sergey Pershin, Sergii Druzkin, Sreeni Viswanadha, Swapnil Tailor, Timothy Meehan, Vivek, Ying, Zac, Zhenxiao Luo, abhiseksaikia, ajantha-bhat, dnnanuti, dnskr, pen4, singcha, suheng, tanjialiang, v-jizhang, wangd, xiaoxmeng
