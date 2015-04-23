=============
Release 0.102
=============

General Changes
---------------

* Support returning booleans as numbers in JDBC driver

Hive Changes
------------

* Collect more metrics from ``PrestoS3FileSystem``.
* Retry when seeking in ``PrestoS3FileSystem``.
* Ignore ``InvalidRange`` error in ``PrestoS3FileSystem``.
* Implement rename and delete in ``PrestoS3FileSystem``.
