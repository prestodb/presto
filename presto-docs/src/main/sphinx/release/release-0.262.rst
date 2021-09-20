=============
Release 0.262
=============

**Highlights**
==============
* Fix a correctness bug which could be triggered for queries with aggregations on partitioning columns and filters on non-partitioning columns when both optimizing
  metadata queries and filter pushdown are enabled.
* Add support for elastic workers by implementing time-to-live based node scheduling for queries with execution time estimates. See :pr:`16409`

**Details**
===========

General Changes
_______________
* Fix :func:`reduce_agg` to allow ``NULL`` as the result of input/combine functions, while also allowing only constant expressions and not ``NULL`` as the initial value.
* Fix a correctness bug which could be triggered for queries with aggregations on partitioning columns and filters on non-partitioning columns when both optimizing
  metadata queries and filter pushdown are enabled.
* Add default size limit (100MB) to build side of broadcast join.
* Add support for elastic workers by implementing time-to-live based node scheduling for queries with execution time estimates. This can be enabled by setting either the
  ``resource_aware_scheduling_strategy`` session property or the ``experimental.resource-aware-scheduling-strategy`` configuration property to ``TTL``. See :pr:`16409`.
* Add adjusted queue size information to the ``v1/cluster`` endpoint.

SPI Changes
___________
* Add support for custom node time-to-live (TTL) fetchers (to fetch TTLs of nodes) and cluster TTL providers (to compute the TTL of a cluster) through the
  ``NodeTtlFetcher`` and ``ClusterTtlProvider`` interfaces respectively. See :pr:`16409`.

Hive Changes
____________
* Fix issue while reading Apache HUDI tables when ``PrestoFileSystemCache`` is refreshed. The issue occurs because ``HoodieROTablePathFilter`` is cached with default
  ``Configuration`` object (:pr:`16611`).
* Add session property ``hive.cache_enabled`` to allow turning on and off data cache per query.

**Credits**
===========

Ajay George, Ariel Weisberg, Arunachalam Thirupathi, Brian Li, Jalpreet Singh Nanda (:imjalpreet), James Petty, James Sun, Neerad Somanchi, Shixuan Fan, Sreeni Viswanadha, Swapnil Tailor, Tal Galili, Timothy Meehan, Vivek, Xiang Fu, Ying, Zac Wen, Zhan Yuan, Zhenxiao Luo, Zhongting Hu, abhiseksaikia
