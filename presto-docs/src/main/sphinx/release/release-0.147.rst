=============
Release 0.147
=============

General Changes
---------------

* Add :doc:`/sql/show-create-view`.
* Support ``LIKE`` clause for :doc:`/sql/show-catalogs` and :doc:`/sql/show-schemas`.

Hive Changes
------------

* Fix reading symlinks when the target is in a different HDFS instance.
* Fix ``NoClassDefFoundError`` for ``SubnetUtils`` in HDFS client.
* Include path in unrecoverable S3 exception messages.
* When replacing an existing Presto view, update the view data
  in the Hive metastore rather than dropping and recreating it.
