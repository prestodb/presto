=============
Release 0.103
=============

Cluster Resource Management
---------------------------

There is a new cluster resource manager, which can be enabled via the
``experimental.cluster-memory-manager-enabled`` flag. Currently, the only
resource that's tracked is memory, and the cluster resource manager guarantees
that the cluster will not deadlock waiting for memory. However, in a low memory
situation it is possible that only one query will make progress. Memory limits can
now be configured via ``query.max-memory`` which controls the total distributed
memory a query may use and ``query.max-memory-per-node`` which limits the amount
of memory a query may use on any one node. On each worker, the
``resources.reserved-system-memory`` flags controls how much memory is reserved
for internal Presto data structures and temporary allocations.

Hive Changes
------------

* Removed the ``hive.max-split-iterator-threads`` parameter and renamed
  ``hive.max-global-split-iterator-threads`` to ``hive.max-split-iterator-threads``.
* Fix excessive object creation when querying tables with a large number of partitions.

General Changes
---------------

* Add :func:`array_remove`.
* Fix NPE in :func:`max_by` and :func:`min_by` caused when few rows were present in the aggregation.
* Reduce memory usage of :func:`map_agg`.
