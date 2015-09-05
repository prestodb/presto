=============
Release 0.100
=============

System Connector
----------------

The :doc:`/connector/system` now works like other connectors: global system
tables are only available in the ``system`` catalog, rather than in a special
schema that is available in every catalog. Additionally, connectors may now
provide system tables that are available within that connector's catalog by
implementing the ``getSystemTables()`` method on the ``Connector`` interface.

General Changes
---------------

* Fix ``%f`` specifier in :func:`date_format` and :func:`date_parse`.
* Add ``WITH ORDINALITY`` support to ``UNNEST``.
* Add :func:`array_distinct` function.
* Add :func:`split` function.
* Add :func:`degrees` and :func:`radians` functions.
* Add :func:`to_base` and :func:`from_base` functions.
* Rename config property ``task.shard.max-threads`` to ``task.max-worker-threads``.
  This property sets the number of threads used to concurrently process splits.
  The old property name is deprecated and will be removed in a future release.
* Fix referencing ``NULL`` values in :ref:`row_type`.
* Make :ref:`map_type` comparable.
* Fix leak of tasks blocked during query teardown.
* Improve query queue config validation.
