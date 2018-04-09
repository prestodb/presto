=============
Release 0.196
=============

General Changes
---------------

* Fix behavior of ``JOIN ... USING`` to conform to standard SQL semantics.
  The old behavior can be restored by setting the ``deprecated.legacy-join-using``
  configuration option or the ``legacy_join_using`` session property.
* Fix memory leak for queries with ``ORDER BY``.
* Fix tracking of query peak memory usage.
* Fix skew in dynamic writer scaling by eagerly freeing memory in the source output
  buffers. This can be disabled by setting ``exchange.acknowledge-pages=false``.
* Fix planning failure for lambda with capture in rare cases.
* Fix decimal precision of ``round(x, d)`` when ``x`` is a ``DECIMAL``.
* Fix returned value from ``round(x, d)`` when ``x`` is a ``DECIMAL`` with
  scale ``0`` and ``d`` is a negative integer. Previously, no rounding was done
  in this case.
* Improve performance of the :func:`array_join` function.
* Improve performance of the :func:`ST_Envelope` function.
* Optimize :func:`min_by` and :func:`max_by` by avoiding unnecessary object
  creation in order to reduce GC overhead.
* Show join partitioning explicitly in ``EXPLAIN``.
* Add :func:`is_json_scalar` function.
* Add :func:`regexp_replace` function variant that executes a lambda for
  each replacement.

Security
--------

* Add rules to the ``file`` :doc:`/security/built-in-system-access-control`
  to enforce a specific matching between authentication credentials and a
  executing username.

Hive Changes
------------

* Fix a correctness issue where non-null values can be treated as null values
  when writing dictionary-encoded strings to ORC files with the new ORC writer.
* Fix invalid failure due to string statistics mismatch while validating ORC files
  after they have been written with the new ORC writer. This happens when
  the written strings contain invalid UTF-8 code points.
* Add support for reading array, map, or row type columns from partitions
  where the partition schema is different from the table schema. This can
  occur when the table schema was updated after the partition was created.
  The changed column types must be compatible. For rows types, trailing fields
  may be added or dropped, but the corresponding fields (by ordinal)
  must have the same name.
* Add ``hive.non-managed-table-creates-enabled`` configuration option
  that controls whether or not users may create non-managed (external) tables.
  The default value is ``true``.
