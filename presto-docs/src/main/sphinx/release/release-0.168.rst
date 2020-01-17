=============
Release 0.168
=============

General Changes
---------------

* Fix correctness issues for certain ``JOIN`` queries that require implicit coercions
  for terms in the join criteria.
* Fix invalid "No more locations already set" error.
* Fix invalid "No more buffers already set" error.
* Temporarily revert empty join short-circuit optimization due to issue with hanging queries.
* Improve performance of ``DECIMAL`` type and operators.
* Optimize window frame computation for empty frames.
* :func:`json_extract` and :func:`json_extract_scalar` now support escaping double
  quotes or backslashes using a backslash with a JSON path subscript. This changes
  the semantics of any invocation using a backslash, as backslashes were previously
  treated as normal characters.
* Improve performance of :func:`filter` and :func:`map_filter` lambda functions.
* Add :doc:`/connector/memory`.
* Add :func:`arrays_overlap` and :func:`array_except` functions.
* Allow concatenating more than two arrays with ``concat()`` or maps with :func:`map_concat`.
* Add a time limit for the iterative optimizer. It can be adjusted via the ``iterative_optimizer_timeout``
  session property or ``experimental.iterative-optimizer-timeout`` configuration option.
* ``ROW`` types are now orderable if all of the field types are orderable.
  This allows using them in comparison expressions, ``ORDER BY`` and
  functions that require orderable types (e.g., :func:`max`).

JDBC Driver Changes
-------------------

* Update ``DatabaseMetaData`` to reflect features that are now supported.
* Update advertised JDBC version to 4.2, which part of Java 8.
* Return correct driver and server versions rather than ``1.0``.

Hive Changes
------------

* Fix reading decimals for RCFile text format using non-optimized reader.
* Fix bug which prevented the file based metastore from being used.
* Enable optimized RCFile reader by default.
* Common user errors are now correctly categorized.
* Add new, experimental, RCFile writer optimized for Presto.  The new writer can be enabled with the
  ``rcfile_optimized_writer_enabled`` session property or the ``hive.rcfile-optimized-writer.enabled``
  Hive catalog property.

Cassandra Changes
-----------------

* Add predicate pushdown for clustering key.

MongoDB Changes
---------------

* Allow SSL connections using the ``mongodb.ssl.enabled`` config flag.

SPI Changes
-----------

* ConnectorIndex now returns ``ConnectorPageSource`` instead of ``RecordSet``.  Existing connectors
  that support index join can use the ``RecordPageSource`` to adapt to the new API.
