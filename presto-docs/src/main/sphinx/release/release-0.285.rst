=============
Release 0.285
=============

.. warning::

   There is a bug for LIKE pattern with multiple-byte characters. (:pr:`21578`)

**Details**
===========

General Changes
_______________
* Fix `min_by(x, y, n) / max_by(x, y, n)` functions to ensure `n` is unique.
* Fix LIKE pattern to handle multi-byte characters. :pr:`21578`
* Improve latency of HBO. :pr:`21297`, :pr:`21308`
* Improve HBO to track partial aggregation statistics. :pr:`21160`
* Improve PushPartialAggregationThroughExchange to use partial aggregation statistics when available. This extends to multi-key aggregations.
* Improve performance of union when subqueries are empty.
* Improve cost based optimizer join reordering to work with non-simple equi-join predicates. :pr:`21153`
* Improve history based optimizer performance when nulls are present in joins. :pr:`21203`
* Add :func:`array_least_frequent` function.
* Add support for HANA connector. :pr:`21034`
* Add task killer which is triggered when a worker is running out of memory and the garbage collector cannot reclaim sufficient memory. Two strategies are provided: full garbage collection, and frequent full garbage collection. :pr:`21254`
* Add support to remove redundant `cast to varchar` expressions in a join condition. This feature is configurable by the session property ``remove_redundant_cast_to_varchar_in_join`` (enabled by default). :pr:`21050`
* Add support to use HBO for scaled writers. This feature is configurable by the session property ``enable_hbo_for_scaled_writer`` (disabled by default).
* Add support to split aggregates into partial and final based on partial aggregation statistics. This feature is configurable by the session property ``use_partial_aggregation_history``(disabled by default). :pr:`21160`
* Add optimization where values node followed by an always false filter is converted to an empty values node.
* Add information about cost-based optimizers and the source of stats they use (CBO/HBO) in explain plans when session property ``verbose_optimizer_info_enabled=true``. :pr:`20990`
* Upgrade Joda-Time to 2.12.5 to use 2023c tzdata. The JVM should also be updated to ensure the timezone data is consistent. :pr:`21329`
* Upgrade Alluxio to 304 from 2.8.1.
* Upgrade AWS SDK to 1.12.560 from 1.12.261.
* Upgrade Avro version to 1.11.3 from 1.9.2.

Prestissimo (Native Execution) Changes
______________________________________
* Fix task cleanup to use task termination time instead of task end time.
* Add support for JWT authentication. :pr:`20290`
* Add session property ``native_debug.validate_output_from_operators`` to identify malformed output from operators. :pr:`21036`

Security Changes
________________
* Fix critical vulnerability in Babel and related npm packages by updating to newer versions. :pr:`21322`

Hive Connector Changes
______________________
* Improve support for schema evolution of partition column types for Parquet file format. :pr:`19983`
* Add support for Parquet writer versions V1 and V2. See :doc:`Hive Connector</connector/hive>`.

Iceberg Connector Changes
_________________________
* Add support for reading and writing distinct value count statistics as described by Iceberg's Puffin file specification. :pr:`20993`
* Add support for ``ANALYZE`` when configured with the Hive Catalog. The table must be un-partitioned. :pr:`20720`
* Add support for ``DELETE FROM <table> [where <filter>]``. Deletes one or more partitions. :pr:`21048`
* Add support for ``ALTER TABLE <table> ADD COLUMN <column> [WITH (partitioning = '<transform_func>')]``. :pr:`21206`
* Add support for creating tables using the AWS Glue metastore. :pr:`20699`
* Add support for ``SHOW STATS`` for tables with ``timestamp`` type. :pr:`21286`
* Add support for views. See :doc:`Iceberg Connector</connector/iceberg>`.
* Add support for Parquet writer versions V1 and V2.
* Remove ``timestamp with time zone`` type in create table and alter table statements. :pr:`21096`
* Upgrade Iceberg version from 1.3.1 to 1.4.1.

Pinot Connector Changes
_______________________
* Fix push down of literal expression with single quotes. :pr:`21020`


**Credits**
===========

Ajay George, Ajay Gupte, Amit Dutta, Anant Aneja, Andrii Rosa, Arjun Gupta, Avinash Jain, Beinan, Bikramjeet Vig, Chandrashekhar Kumar Singh, Christian Zentgraf, Chunxu Tang, Deepak Majeti, Eduard Tudenhoefner, James Xu, Jialiang Tan, JiamingMai, Jimmy Lu, Jonathan Hehir, Karteekmurthys, Ke, Kevin Wilfong, Krishna Pai, Lyublena Antova, Mahadevuni Naveen Kumar, Masha Basmanova, Michael Shang, Miguel Blanco Godón, Nikhil Collooru, Pedro Pedreira, Pranjal Shankhdhar, Pratyush Verma, Ruslan Mardugalliamov, Sergey Pershin, Sergii Druzkin, Shrinidhi Joshi, Sotirios Delimanolis, Sreeni Viswanadha, Steve Burnett, Sudheesh, Swapnil Tailor, Tim Meehan, Xiang Fu, Yihong Wang, Zac Blanco, aditi-pandit, feilong-liu, kedia,Akanksha, kiersten-stokes, mmorgan98, pratyakshsharma, wangd, wypb, xiaoxmeng, yingsu00
