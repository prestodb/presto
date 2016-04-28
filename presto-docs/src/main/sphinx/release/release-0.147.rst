=============
Release 0.147
=============

Hive Changes
------------

* Fix ``NoClassDefFoundError`` for ``SubnetUtils`` in HDFS client.
* Include path in unrecoverable S3 exception messages.
* When replacing an existing Presto view, update the view data
  in the Hive metastore rather than dropping and recreating it.
