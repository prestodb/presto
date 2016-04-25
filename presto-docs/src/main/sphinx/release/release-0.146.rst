=============
Release 0.146
=============

General Changes
---------------

* Require at least 4096 file descriptors to run Presto.

Hive Changes
------------

* Fix incorrect skipping of data in Parquet during predicate push-down.
* Fix reading of Parquet maps and lists containing nulls.
* Fix reading empty ORC file with ``hive.orc.use-column-names`` enabled.
* Legacy authorization properties, such as ``hive.allow-drop-table``, are now
  only enforced when ``hive.security=none`` is set, which is the default
  security system. Specifically, the ``sql-standard`` authorization system
  does not enforce these settings.
