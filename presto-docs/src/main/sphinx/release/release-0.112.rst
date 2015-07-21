=============
Release 0.112
=============

General Changes
---------------

* Fix handling of ``LIMIT`` when used with a partitioned :func:`row_number`.
* Fix non-string object arrays in JMX connector.

Hive Changes
------------

* Tables created using :doc:`/sql/create-table` (not :doc:`/sql/create-table-as`)
  had invalid metadata and were not readable.
