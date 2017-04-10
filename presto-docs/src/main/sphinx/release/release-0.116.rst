=============
Release 0.116
=============

Cast between JSON and VARCHAR
-----------------------------

Casts of both directions between JSON and VARCHAR have been removed. If you
have such casts in your scripts or views, they will fail with a message when
you move to release 0.116. To get the semantics of the current casts, use:

* `JSON_PARSE(x)` instead of `CAST(x as JSON)`
* `JSON_FORMAT(x)` instead of `CAST(x as VARCHAR)`

In a future release, we intend to reintroduce casts between JSON and VARCHAR
along with other casts involving JSON. The semantics of the new JSON and
VARCHAR cast will be consistent with the other casts being introduced. But it
will be different from the semantics in 0.115 and before. When that comes,
cast between JSON and VARCHAR in old scripts and views will produce unexpected
result.

Cluster Memory Manager Improvements
-----------------------------------

The cluster memory manager now has a low memory killer. If the cluster runs low
on memory, the killer will kill queries to improve throughput. It can be enabled
with the ``query.low-memory-killer.enabled`` config flag, and the delay between
when the cluster runs low on memory and when the killer will be invoked can be
configured with the ``query.low-memory-killer.delay`` option.

General Changes
---------------

* Add :func:`multimap_agg` function.
* Add :func:`checksum` function.
* Add :func:`max` and :func:`min` that takes a second argument and produces
  ``n`` largest or ``n`` smallest values.
* Add ``query_max_run_time`` session property and ``query.max-run-time``
  config. Queries are failed after the specified duration.
* Removed ``experimental.cluster-memory-manager-enabled`` config. The cluster
  memory manager is now always enabled.
* Removed ``task.max-memory`` config.
* ``optimizer.optimize-hash-generation`` and ``distributed-joins-enabled`` are
  both enabled by default now.
* Add optimization for ``IF`` on a constant condition.
