=============
Release 0.101
=============

General Changes
---------------

* Add support for :doc:`/sql/create-table` (in addition to :doc:`/sql/create-table-as`).
* Add :func:`array_agg` function.
* Add :func:`array_intersect` function.
* Add :func:`regexp_split` function.
* Fix analysis of ``UNION`` queries for tables with hidden columns.

Hive Changes
------------

* Add support for connecting to S3 using EC2 instance credentials.
  This feature is enabled by default. To disable it, set
  ``hive.s3.use-instance-credentials=false`` in your Hive catalog properties file.
* Treat ORC files as splittable.
* Lower the Hive metadata refresh interval from two minutes to one second.
* Invalidate Hive metadata cache for failed operations.
* Support ``s3a`` file system scheme.
