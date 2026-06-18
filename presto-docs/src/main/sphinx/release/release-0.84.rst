============
Release 0.84
============

* Fix handling of ``NaN`` and infinity in ARRAYs
* Fix approximate queries that use ``JOIN``
* Reduce excessive memory allocation and GC pressure in the coordinator
* Fix an issue where setting ``node-scheduler.location-aware-scheduling-enabled=false``
  would cause queries to fail for connectors whose splits were not remotely accessible
* Fix error when running ``COUNT(*)`` over tables in ``information_schema`` and ``sys``
