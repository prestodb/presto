=============
Release 0.264
=============

**Highlights**
==============
* Add support in Iceberg connector for a native mode that can be used without a Hive installation, to run queries against Iceberg native catalogs

**Details**
===========

General Changes
_______________
* Add :func:`murmur3_x64_128` UDF that computes a hash equivalent to MurmurHash3_x64_128 (Murmur3F) in C++.
* Add a new configuration property ``experimental.dedup-based-distinct-aggregation-spill-enabled`` to enable deduplication of input data before spilling for distinct aggregates. This can be overridden by ``dedup_based_distinct_aggregation_spill_enabled`` session property.
* Add configuration properties ``simple-ttl-node-selector.use-default-execution-time-estimate-as-fallback`` and ``simple-ttl-node-selector.default-execution-time-estimate``. The former configures ``SimpleTtlNodeSelector`` to use a default execution time estimate when there is no corresponding user-provided estimate. The latter configures the value of the default execution time estimate.
* Add support for running task count ``runningTasks`` at the cluster level via ``v1/cluster`` endpoint.

Hive Changes
____________
* Add session property ``hive.metastore_headers`` to allow the users to set headers that will be used in metastore operations.

Iceberg Changes
____________
* Add support in Iceberg connector for a native mode that can be used without a Hive installation, to run queries against Iceberg  native catalogs.

**Credits**
===========

Arjun Gupta, Arunachalam Thirupathi, Basar Hamdi Onat, Beinan, Ge Gao, Hanumath Rao Maduri, Hitarth Trivedi, Jack Ye, James Petty, James Sun, Maria Basmanova, Neerad Somanchi, Pranjal Shankhdhar, Reetika Agrawal, Rongrong Zhong, Shrinidhi Joshi, Sreeni Viswanadha, Swapnil Tailor, Tim Meehan, Zac Wen, Zhan Yuan, abhiseksaikia, linzebing, tanjialiang, zhaoyulong
