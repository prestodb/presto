=============
Release 0.113
=============

Cluster Resource Management
---------------------------

The cluster resource manager announced in :doc:`/release/release-0.103` is now enabled by default.
You can disable it with the ``experimental.cluster-memory-manager-enabled`` flag.
Memory limits can now be configured via ``query.max-memory`` which controls the total distributed
memory a query may use and ``query.max-memory-per-node`` which limits the amount
of memory a query may use on any one node. On each worker, the
``resources.reserved-system-memory`` config property controls how much memory is reserved
for internal Presto data structures and temporary allocations.

General Changes
---------------

* Add :func:`element_at` function.
