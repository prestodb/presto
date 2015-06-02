=============
Release 0.106
=============

General Changes
---------------

* Parallelize startup of table scan task splits.
* Fixed index join driver resource leak.
* Improve CPU efficiency of coordinator.
* Added ``Asia/Chita``, ``Asia/Srednekolymsk``, and ``Pacific/Bougainville`` time zones.
* Fix task leak caused by race condition in stage state machine.
* Fix blocking in Hive split source.
