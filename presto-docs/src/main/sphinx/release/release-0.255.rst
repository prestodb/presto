=============
Release 0.255
=============

**Details**
===========

General Changes
_______________
* Fix an issue where regular expression functions were not interruptible and could keep running for a long time after the query they were a part of failed.
* Add support to interrupt runaway splits blocked in known situations. The interrupt timeout can be configured by the configuration property ``task.interrupt-runaway-splits-timeout``. The default value is ``600s``.
* Added :func:`array_normalize` function to normalize an array by dividing each element by the p-norm of it.

JDBC Changes
____________
* Added connection param ``protocols`` that allows a user to specify which HTTP protocols the Presto client is allowed to use.

Cassandra Connector Changes
___________________________
* Add support for Cassandra ``SMALLINT``, ``TINYINT`` and ``DATE`` types.

Druid Connector Changes
_______________________
* Add support for querying non-lowercase table names in Druid connector (:pr:`15920`).

Elasticserarch Changes
______________________
* Fix to avoid ``NullPointerException`` when there is an unsupported data type column in the Object field.

Hive Changes
____________
* Fix the import of ``SMALLINT`` in ParquetReader.
* Revert non-backward-compatible DWRF writer updates (:pr:`16037`)
* Add support for static AWS credentials in GlueHiveMetastore :doc:`/connector/hive`.

**Contributors**
================

Abhisek Gautam Saikia, Amit Adhikari, Andrew Donley, Andrii Rosa, Ariel Weisberg, Arunachalam Thirupathi, Basar Hamdi Onat, Chen, Chunxu Tang, Darren Fu, Jalpreet Singh Nanda (:imjalpreet), James Petty, James Sun, Julian Zhuoran Zhao, Maria Basmanova, Mayank Garg, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Rongrong Zhong, Sergii Druzkin, Shixuan Fan, Sreeni Viswanadha, Tim Meehan, Venki Korukanti, Zhan Yuan, Zhenxiao Luo, beinan, henneberger, prithvip, v-jizhang
