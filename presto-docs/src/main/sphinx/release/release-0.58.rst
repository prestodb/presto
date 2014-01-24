============
Release 0.58
============

* Add first version of Cassandra connector. This plugin is still in
  development and is not yet bundled with the server. See the ``README``
  in the plugin source directory for details.

* Support UDFs for internal plugins. This is not yet part of the SPI
  and is a stopgap feature intended for advanced users. UDFs must be
  implemented using the internal Presto APIs which often change
  substantially between releases.

* Fix Hive connector semaphore release bug.

* Fix handling of non-splittable files without blocks.
