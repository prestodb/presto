=============
Release 0.224
=============

General Changes
---------------

* Fix an issue where CPU time and scheduled time are swapped in query plans.
* Fix a correctness issue for queries containing nested coercible type casts.
* Add support for recoverable grouped execution when writing unpartitioned table.
* Add support for index-based access to fields of type ``ROW`` with subscript.

Security Changes
----------------

* Fix a security issue in the client protocol.

Web UI Changes
--------------

* Add ``Planning Time`` in ``Query Details`` page.

Hive Connector Changes
----------------------

* Improve performance for bucketed table insertion when some buckets are empty.
* Add config property ``hive.max-buckets-for-grouped-execution`` and session
  property ``max_buckets_for_grouped_execution`` to limit the number of buckets
  a query can access while still taking the advantage of grouped execution.
  If more than the configured number of buckets are queried, the query will
  run without grouped execution.
* Add session property ``hive.virtual_bucket_count`` to support grouped
  execution for queries that read unbucketed tables and have no joins or
  aggregations. This session property controls the number of virtual buckets
  that the connector will create for unbucketed tables.

Raptor Connector Changes
------------------------

* Fix query failures on colocated joins when one table has empty data.

Verifier Changes
----------------

* Add the reason that a test case is skipped to the output event.
