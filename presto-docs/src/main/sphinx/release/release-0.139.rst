=============
Release 0.139
=============

General Changes
---------------

* Fix planning bug that causes some joins to not be redistributed when
  ``distributed-joins-enabled`` is true.

Hive Changes
------------

* Remove cursor-based readers for ORC and DWRF file formats, as they have been
  replaced by page-based readers.
* Fix query failure in ``CREATE TABLE AS SELECT`` in Hive connector when S3
  is the storage backend.
