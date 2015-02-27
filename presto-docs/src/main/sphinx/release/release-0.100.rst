=============
Release 0.100
=============

General Changes
---------------
* Add :func:`array_distinct` function.
* Add :func:`split` function.
* Add :func:`degrees` and :func:`radians` functions.
* Add :func:`to_base` and :func:`from_base` functions.
* Rename config property ``task.shard.max-threads`` to ``task.max-worker-threads``.
  This property sets the number of threads used to concurrently process splits.
  The old property name is deprecated and will be removed in a future release.
