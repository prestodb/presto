=============
Release 0.138
=============

General Changes
---------------

* Fix planning bug with ``NULL`` literal coercions.
* Reduce query startup time by reducing lock contention in scheduler.
* Fix query failure in ``CREATE TABLE AS SELECT`` in Hive connector when S3
  is the storage backend.
