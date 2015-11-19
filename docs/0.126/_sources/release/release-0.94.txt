============
Release 0.94
============

ORC Memory Usage
----------------

This release contains additional changes to the Presto ORC reader to favor
small buffers when reading varchar and varbinary data. Some ORC files contain
columns of data that are hundreds of megabytes compressed. When reading these
columns, Presto would allocate a single buffer for the compressed column data,
and this would cause heap fragmentation in CMS and G1 and eventually OOMs.
In this release, the ``hive.orc.max-buffer-size`` sets the maximum size for a
single ORC buffer, and for larger columns we instead stream the data. This
reduces heap fragmentation and excessive buffers in ORC at the expense of
HDFS IOPS. The default value is ``8MB``.

General Changes
---------------

* Update Hive CDH 4 connector to CDH 4.7.1
* Fix ``ORDER BY`` with ``LIMIT 0``
* Fix compilation of ``try_cast``
* Group threads into Java thread groups to ease debugging
* Add ``task.min-drivers`` config to help limit number of concurrent readers
