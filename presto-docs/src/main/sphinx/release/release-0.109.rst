=============
Release 0.109
=============

General Changes
---------------

* Add :func:`slice`, :func:`md5`, :func:`array_min` and :func:`array_max` functions.
* Fix bug that could cause queries submitted soon after startup to hang forever.
* Improve index join planning heuristics to favor streaming execution.
* Improve validation of date/time literals.
* Produce RPM package for Presto server.
* Add redistribution before table writes, unless the session property
  ``redistribute_writes`` or the config property ``redistribut-writes`` is set
  to false.

Remove "Big Query" Support
--------------------------
The experimental support for big queries has been removed in favor of
the new resource manager which can be enabled via the
``experimental.cluster-memory-manager-enabled`` config option.
The ``experimental_big_query`` session property and the following config
options are no longer supported: ``experimental.big-query-initial-hash-partitions``,
``experimental.max-concurrent-big-queries`` and ``experimental.max-queued-big-queries``.
