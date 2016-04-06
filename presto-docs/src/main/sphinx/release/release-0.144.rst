=============
Release 0.144
=============

General Changes
---------------

* Fix already exists check when adding a column to be case-insensitive.
* Fix correctness issue when complex grouping operations have a partitioned source.
* Fix potential memory starvation when a query is run with
  ``resource_overcommit=true``.
* Queries run with ``resource_overcommit=true`` may now be killed before
  they reach ``query.max-memory``, if the cluster is low on memory.
* Add support for :doc:`/sql/explain-analyze`.
