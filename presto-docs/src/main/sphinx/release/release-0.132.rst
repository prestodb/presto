=============
Release 0.132
=============

Hive Changes
------------

* Change unit of Hive namenode latency JMX stats from seconds to milliseconds.
* Fix ``NullPointerException`` when inserting a null value for a partition column.
* Fix a correctness issue that can occur when any join depends on the output
  of another outer join that has an inner side (or either side for the full outer
  case) for which the connector declares that it has no data during planning.
