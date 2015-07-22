=============
Release 0.112
=============

General Changes
---------------

* Fix incorrect handling of filters and limits in :func:`row_number` optimizer.
  This caused certain query shapes to produce incorrect results.
* Fix non-string object arrays in JMX connector.

Hive Changes
------------

* Tables created using :doc:`/sql/create-table` (not :doc:`/sql/create-table-as`)
  had invalid metadata and were not readable.
* Improve performance of ``IN`` and ``OR`` clauses when reading ``ORC`` data.
  Previously, the ranges for a column were always compacted into a single range
  before being passed to the reader, preventing the reader from taking full
  advantage of row skipping. The compaction only happens now if the number of
  ranges exceeds the ``hive.domain-compaction-threshold`` config property.
