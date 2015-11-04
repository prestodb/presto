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

Task Parallelism
----------------
Queries involving a large number of aggregations or a large hash table for a
join can be slow due to single threaded execution in the intermediate stages.
This release adds experimental configuration and session properties to execute
this single threaded work in parallel.  Depending on the exact query this may
reduce wall time, but will likely increase CPU usage.

Use the configuration parameter ``task.default-concurrency`` or the session
property ``task_default_concurrency`` to set the default number of parallel
workers to use for join probes, hash builds and final aggregations.
Additionally, the session properties ``task_join_concurrency``,
``task_hash_build_concurrency`` and ``task_aggregation_concurrency`` can be
used to control the parallelism for each type of work.

This is an experimental feature and will likely change in a future release.  It
is also expected that this will eventually be handled automatically by the
query planner and these options will be removed entirely.

Hive Changes
------------

* Removed the ``hive.max-split-iterator-threads`` parameter and renamed
  ``hive.max-global-split-iterator-threads`` to ``hive.max-split-iterator-threads``.
* Fix excessive object creation when querying tables with a large number of partitions.
* Do not retry requests when an S3 path is not found.

General Changes
---------------

* Add :func:`array_remove`.
* Fix NPE in :func:`max_by` and :func:`min_by` caused when few rows were present in the aggregation.
* Reduce memory usage of :func:`map_agg`.
* Change HTTP client defaults: 2 second idle timeout, 10 second request
  timeout and 250 connections per host.
* Add SQL command autocompletion to CLI.
* Increase CLI history file size.
