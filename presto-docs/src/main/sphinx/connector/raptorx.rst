=======
RaptorX
=======
.. contents::
  :local:
  :backlinks: none
  :depth: 1

Overview
--------

RaptorX boosts query latency using a hierarchical cache work architecture to beat performance oriented connectors like Raptor with the added benefit of continuing to work with disaggregated storage.

Problems with Disaggregated Storage
-----------------------------------

`Disaggregated storage <https://en.wikipedia.org/wiki/Disaggregated_storage>`_ helps cloud providers reduce costs. Presto supports such architecture by streaming data from remote storage nodes outside of Presto servers.

Storage-compute disaggregation provides challenges for query latency, as scanning huge amounts of data over the wire can become IO bound when the network is saturated. Because the metadata paths also go through the wire to retrieve the location of the data, a few roundtrips of metadata RPCs can increase the latency to more than a second. The following figure shows the IO paths for Hive connectors in orange color, each of which could be a bottleneck on query performance.

.. image:: https://prestodb.io/wp-content/uploads/presto-arch-2048x1103.png
  :align: center
  :width: 800px
  :alt: presto-arch image

RaptorX: Build a Hierarchical Caching Solution
----------------------------------------------

To solve the network saturation issue, Presto uses a built-in Raptor connector to load data from remote storage to local SSD for fast access. However, this solution is no different from having a shared compute/storage node which runs counter to the idea of storage-compute disaggregation. Either Presto wastes CPU because the SSD in a worker is full, or Presto wastes SSD capacity if CPU bound. 

RaptorX uses hierarchical caching, particularly useful when storage nodes are disaggregated from compute nodes, as a built-in solution for existing workloads to benefit without migration. RaptorX targets the existing Hive connector which is used by many workloads.

The following figure shows the architecture of the caching solution and its hierarchical layers of cache:

* Metastore versioned cache: cache table and partition information in the coordinator. Because metadata is mutable such as in Iceberg or Delta Lake, the information is versioned. Sync versions with metastore and fetch updated metadata only when a version is out of date.
* File list cache: Cache file list from remote storage partition directory.
* Fragment result cache: Cache a partially computed result on leaf worker’s local SSDs. Pruning techniques are required to simplify query plans as they often change.
* File handle and footer cache: Cache open file descriptors and stripe/file footer information in leaf worker memory. These pieces of data are frequently accessed when reading files.
* Alluxio data cache: Cache file segments with 1MB aligned chunks on leaf worker’s local SSDs. The library is built from Alluxio’s cache service.
* Affinity scheduler: A scheduler that sends sticky requests based on file paths to particular workers to maximize cache hit rate.

.. image:: https://prestodb.io/wp-content/uploads/raptorx-arch-1024x692.png
  :align: center
  :width: 800px
  :alt: raptorx-arch image

Metastore Versioned Cache
-------------------------
.. role:red

A Presto coordinator caches table metadata (schema, partition list, and partition info) to avoid long ``getPartitions`` calls to Hive Metastore. Because Hive table metadata is mutable, versioning is needed to determine if the cached metadata is valid or not. The coordinator attaches a version number to each cache key-value pair. When a read request comes, the coordinator asks the Hive Metastore to get the partition info - if it’s not cached at all - or checks with Hive Metastore to confirm that the cached info is up to date. The roundtrip to Hive Metastore cannot be avoided, but the version matching is relatively cheap when compared with fetching the entire partition info.

File List cache
---------------

A Presto coordinator caches file lists in memory to avoid long ``listFile`` calls to remote storage. This can only be applied to sealed directories. For open partitions, Presto skips caching those directories to guarantee data freshness. One major use case for open partitions is to support the need of near-realtime ingestion and serving. In such cases, the ingestion engines (for example, micro batch) keep writing new files to the open partitions so that Presto can read near-realtime data. 

Fragment Result Cache
---------------------

A Presto worker that is running a leaf stage can cache the partially computed results on local SSD to prevent duplicated computation upon multiple queries. A typical use case is to cache the plan fragments on leaf stage with one level of scan, filter, project, or aggregation.

For example, a user sends the following query where ``ds`` is a partition column:

.. code-block:: sql

    SELECT SUM(col) FROM T WHERE ds BETWEEN '2021-01-01' AND '2021-01-03'

The partially computed sum for the the corresponding files for each of ``2021-01-01``, ``2021-01-02``, and ``2021-01-03`` partitions is cached on leaf workers forming a fragment result. 

The user sends another query:

.. code-block:: sql

    SELECT sum(col) FROM T WHERE ds BETWEEN '2021-01-01' AND '2021-01-05'

The leaf worker directly fetches the fragment result for ``2021-01-01``, ``2021-01-02``, and ``2021-01-03`` from cache and computes the partial sum for ``2021-01-04`` and ``2021-01-05``.

The fragment result is based on the leaf query fragment, which could vary greatly as users add or remove filters or projections. The above example shows how to handle filters with only partition columns. Partition statistics based pruning avoids the cache miss caused by changing non-partition column filters. Consider the following query, where time is a non-partition column:

.. code-block:: sql

    SELECT SUM(col) FROM T
    WHERE ds BETWEEN '2021-01-01' AND '2021-01-05'
    AND time > now() - INTERVAL '3' DAY

Note that ``now()`` is a function that has constantly changing values changing. 

* If a leaf worker caches the plan fragment based on the absolute value of ``now()``, there is almost no chance to have a cache hit. 
* If predicate ``time > now() - INTERVAL '3' DAY`` is a loose condition that is true for most of the partitions, the predicate can be stripped out from the plan during scheduling time. 

For example, if today was ``2021-01-04``, we know for partition ``ds = 2021-01-04``, predicate ``time > now() - INTERVAL '3' DAY`` is always true.

More generically, consider the following figure that contains a predicate and 3 partitions (``A, B, C``) with stats showing min and max. When the partition stats domain has no overlap with the predicate domain (for example, partition ``A``), this partition can be directly pruned without sending splits to workers. If the partition stats is completely contained by the predicate domain (for example, Partition ``C``), then this predicate is always true for this specific partition and the predicate can be stripped when doing plan comparison. For other partitions that have some overlapping with the predicate, the partition must be scanned using the given filter.

.. image:: https://prestodb.io/wp-content/uploads/frc-1024x146.png
  :align: center
  :width: 800px
  :alt: frc image

File Descriptor and Footer Cache
--------------------------------

A Presto worker caches the file descriptors in memory to avoid long ``openFile`` calls to remote storage, and caches common columnar file and stripe footers in memory. The current supported file formats are ORC, DWRF, and Parquet. These file descriptors are cached in memory because of the high hit rate of footers, which are indexes to the data itself.

Alluxio Data Cache
------------------

Alluxio data cache was introduced in `Improving Presto Latencies with Alluxio Data Caching <https://prestodb.io/blog/2020/06/16/alluxio-datacaching/>`_. It is the main feature to deprecate the Raptor connector. A Presto worker caches remote storage data in its original form (compressed and possibly encrypted) on local SSD upon read. If, in the future, there is a read request covering the range that can be found on the local SSD, the request will return the result directly from the local SSD. The caching library was built as a joint effort with `Alluxio <https://www.alluxio.io/>`_ and the Presto open source community.

The caching mechanism aligns each read into 1MB chunks, where 1MB is configurable to be adapted to different storage capability. For example, Presto issues a read for the 3MB in length starting with offset 0, then Alluxio cache checks if 0 – 1MB, 1 – 2MB, and 2 – 3MB chunks are already on disk and only fetches those that are not cached. The purging policy is based on LRU. It removes chunks from a disk that has not been accessed for the longest time. The Alluxio data cache exposes a HDFS interface to the Hive connector to transparently store requested chunks in a high-performance, highly concurrent, and fault-tolerant storage engine which is designed to serve workloads at scale.


.. image:: https://prestodb.io/wp-content/uploads/alluxio-1024x313.png
  :align: center
  :width: 800px
  :alt: alluxio image

Soft Affinity Scheduling
------------------------

To maximize the cache hit rate on workers, the coordinator needs to schedule the requests for the same file to the same worker because there is a high chance that part of the file has already been cached on that worker. The scheduling policy is “soft”: if the destination worker is too busy or unavailable, the scheduler will fallback to its secondary pick worker for caching or skip the cache when necessary. `Improving Presto Latencies with Alluxio Data Caching <https://prestodb.io/blog/2020/06/16/alluxio-datacaching/>`_ explains the scheduling policy, which guarantees that cache is not on the critical path, but still can boost performance.

Performance
-----------

To compare RaptorX performance with Presto, a test ran a TPC-H benchmark using TPC-H tables with a scale factor of 100 in remote storage on a 114-node cluster. Each worker had a 1TB local SSD with 4 threads configured per task. The following chart shows the comparison between Presto and Presto with the hierarchical cache.

.. image:: https://prestodb.io/wp-content/uploads/tpch-1024x474.png
  :align: center
  :width: 800px
  :alt: alluxio image

From the benchmark, scan-heavy or aggregation-heavy queries like Q1, Q6, Q12 – Q16, Q19, and Q22 all had more than 10X latency improvement. Join-heavy queries like Q2, Q5, Q10, or Q17 had 3X – 5X latency improvements.

User Guide
----------

Worker nodes must have local SSD storage to fully enable this feature. 

To enable various layers of the caches, set the following configs accordingly.

Scheduling (``/catalog/hive.properties``):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: none

    hive.node-selection-strategy=SOFT_AFFINITY

Metastore versioned cache (``/catalog/hive.properties``):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.partition-versioning-enabled=true
    hive.metastore-cache-scope=PARTITION
    hive.metastore-cache-ttl=2d
    hive.metastore-refresh-interval=3d
    hive.metastore-cache-maximum-size=10000000


List files cache (``/catalog/hive.properties``):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.file-status-cache-expire-time=24h
    hive.file-status-cache-size=100000000
    hive.file-status-cache-tables=*

Data cache (``/catalog/hive.properties``):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none
    cache.enabled=true
    cache.base-directory=file:///mnt/flash/data
    cache.type=ALLUXIO
    cache.alluxio.max-cache-size=1600GB

Fragment result cache (``/config.properties and /catalog/hive.properties``):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    fragment-result-cache.enabled=true
    fragment-result-cache.max-cached-entries=1000000
    fragment-result-cache.base-directory=file:///mnt/flash/fragment
    fragment-result-cache.cache-ttl=24h
    hive.partition-statistics-based-optimization-enabled=true

File and stripe footer cache (``/catalog/hive.properties``):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For ORC or DWRF:

.. code-block:: none

    hive.orc.file-tail-cache-enabled=true
    hive.orc.file-tail-cache-size=100MB
    hive.orc.file-tail-cache-ttl-since-last-access=6h
    hive.orc.stripe-metadata-cache-enabled=true
    hive.orc.stripe-footer-cache-size=100MB
    hive.orc.stripe-footer-cache-ttl-since-last-access=6h
    hive.orc.stripe-stream-cache-size=300MB
    hive.orc.stripe-stream-cache-ttl-since-last-access=6h

For Parquet:

.. code-block:: none

    hive.parquet.metadata-cache-enabled=true
    hive.parquet.metadata-cache-size=100MB
    hive.parquet.metadata-cache-ttl-since-last-access=6h


For the original blog post and its contributors that this documentation was adapted from, see `RaptorX: Building a 10X Faster Presto <https://prestodb.io/blog/2021/02/04/raptorx>`_.
