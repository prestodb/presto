============
Release 0.90
============

General Changes
---------------
* Add property ``task.writer-count`` to configure the number of writers per task.
* Added :doc:`/sql/set-session`, :doc:`/sql/reset-session` and :doc:`/sql/show-session`
* Replace ``min(boolean)`` with :func:`bool_and`.
* Replace ``max(boolean)`` with :func:`bool_or`.
* Add standard SQL function :func:`every` as an alias for :func:`bool_and`.
* Add :func:`year_of_week` function.
* Add :func:`regexp_extract_all` function.
* Add :func:`map_agg` function.
* Add support for casting ``JSON`` to ``ARRAY`` or ``MAP`` types.
* Fix failure when accessing elements in an empty array.
* Update the :func:`approx_distinct` documentation with correct standard error bounds.
* Add presto version to query creation and completion events.
* Fix *"Remote page is too large"* errors
* Improve formatting of ``EXPLAIN (TYPE DISTRIBUTED)`` output and include additional
  information such as output layout, task placement policy and partitioning functions.
* Improve error message when attempting to cast a value to ``UKNOWN``.
* Fix a bug when unnesting an array of doubles containing NaN or Infinity.
* Improve planning of UNION queries.
* Initial support for partition and placement awareness in the query planner. This can
  result in better plans for queries involving ``JOIN`` and ``GROUP BY`` over the same
  key columns.
* Fix a bug when optimizing constant expressions involving binary types.
* Add support for unparenthesized expressions in ``VALUES`` clause.
* Fix bug where a table writer commits partial results while cleaning up a failed query.

* Disable falling back to the interpreter when expressions fail to be compiled
  to bytecode. To enable this option, add ``compiler.interpreter-enabled=true``
  to the coordinator and worker config properties. Enabling this option will
  allow certain queries to run slowly rather than failing.

* Improve :doc:`/installation/jdbc` conformance. In particular, all unimplemented
  methods now throw ``SQLException`` rather than ``UnsupportedOperationException``.

Hive Changes
------------
* Disable optimized metastore partition fetching for non-string partition keys.
  This fixes an issue were Presto might silently ignore data with non-canonical
  partition values. To enable this option, add ``hive.assume-canonical-partition-keys=true``
  to the coordinator and worker config properties.
* Don't retry operations against S3 that fail due to lack of permissions.

SPI Changes
-----------
* Add ``getColumnTypes`` to ``RecordSink``.
* Use ``Slice`` for table writer fragments.
* Add ``ConnectorPageSink`` which is a more efficient interface for column-oriented sources.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
