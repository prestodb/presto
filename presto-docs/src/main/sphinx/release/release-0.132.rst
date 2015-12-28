=============
Release 0.132
=============

Hive Changes
------------

* Report metastore and namenode latency in milliseconds rather than seconds in
  JMX stats.
* Fix ``NullPointerException`` when inserting a null value for a partition column.
