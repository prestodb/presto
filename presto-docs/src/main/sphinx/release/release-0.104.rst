=============
Release 0.104
=============

General Changes
---------------

* Handle thread interruption in StatementClient.
* Fix CLI hang when server becomes unreachable during a query.

Hive Changes
------------

* Upgrade to Parquet 1.6.0.
* Collect request time in ``PrestoS3FileSystem``.
