=============
Release 0.146
=============

General Changes
---------------

* Fix error in :func:`map_concat` when the second map is empty.
* Require at least 4096 file descriptors to run Presto.
* Support casting between map types.
* Add :doc:`/connector/mongodb`.

Hive Changes
------------

* Fix incorrect skipping of data in Parquet during predicate push-down.
* Fix reading of Parquet maps and lists containing nulls.
* Fix reading empty ORC file with ``hive.orc.use-column-names`` enabled.
* Fix writing to S3 when the staging directory is a symlink to a directory.
* Legacy authorization properties, such as ``hive.allow-drop-table``, are now
  only enforced when ``hive.security=none`` is set, which is the default
  security system. Specifically, the ``sql-standard`` authorization system
  does not enforce these settings.

Black Hole Changes
------------------

* Add support for ``varchar(n)``.

Cassandra Changes
-----------------

* Add support for Cassandra 3.0.
