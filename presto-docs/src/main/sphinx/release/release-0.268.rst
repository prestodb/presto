=============
Release 0.268
=============

**Details**
===========

General Changes
_______________
* Add support to use consistent hashing as a hash strategy when affinity scheduling is used. This feature can be turned on by setting ``node-scheduler.node-selection-hash-strategy`` to ``CONSISTENT``. A separate configuration ``node-scheduler.consistent-hashing-min-virtual-node-count`` specifies the minimal number of virtual nodes in the consistent hashing ring.

Hive Changes
____________
* Add support for in-memory orc stripe row group index caching. This feature is disabled by default. To enable, set ``orc.row-group-index-cache-enabled`` to ``true``. The cache size can be configured with ``orc.row-group-index-cache-size``, and cache TTL can be configured with ``orc.row-group-index-cache-ttl-since-last-access``.
* Support Hive scalar function.

Iceberg Changes
_______________
* Add support for in-memory orc stripe row group index caching. This feature is disabled by default. To enable, set ``orc.row-group-index-cache-enabled`` to ``true``. The cache size can be configured with ``orc.row-group-index-cache-size``, and cache TTL can be configured with ``orc.row-group-index-cache-ttl-since-last-access``.

Pinot Changes
_____________
* Adding config `pinot.extra-grpc-metadata` to allow customized gRPC request metadata.
* Adding config `pinot.override-distinct-count-function` and session config `override_distinct_count_function` to override ``distinctCount`` function name.
* Adding configs ``pinot.use-https-for-controller/broker/proxy`` to allow using https for Pinot REST APIs.

**Credits**
===========

Ajay George, Alex Ryckman Mellnik, Arunachalam Thirupathi, Beinan Wang, Chen, Jack Ye, James Sun, Neerad Somanchi, Pranjal Shankhdhar, Reetika Agrawal, Rongrong Zhong, SOURAV PAL, Sagar Sumit, Sreeni Viswanadha, Swapnil Tailor, Thejas Viswanathan, Timothy Meehan, Xiang Fu, Ying Su, Zac, Zhan Yuan, Zhenxiao Luo, mengdilin, tanjialiang, zhaoyulong
