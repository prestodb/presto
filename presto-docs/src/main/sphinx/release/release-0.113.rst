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

* Allow using any type with value window functions :func:`first_value`,
  :func:`last_value`, :func:`nth_value`, :func:`lead` and :func:`lag`.
* Add :func:`element_at` function.
* Add :func:`url_encode` and :func:`url_decode` functions.
* :func:`concat` now allows arbitrary number of arguments.
* Fix JMX connector. In the previous release it always returned zero rows.

Hive Changes
------------

* Fix the Hive metadata cache to properly handle negative responses.
  This makes the background refresh work properly by clearing the cached
  metadata entries when an object is dropped outside of Presto.
  In particular, this fixes the common case where a table is dropped using
  Hive but Presto thinks it still exists.
* Fix metastore socket leak when SOCKS connect fails.
