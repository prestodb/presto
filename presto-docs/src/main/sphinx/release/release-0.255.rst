=============
Release 0.255
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix an issue where regular expression functions were not interruptible and could  keep running for a long time after the query they were a part of failed.
* Add interruption for runaway splits blocked in known situations controlled by ``task.interrupt-runaway-splits-timeout`` property which defaults to ``600s``.
* Add support for querying non-lowercase table names in Druid connector (:pr:`15920`).
* Added array_normalize function.
* Added connection param "protocols" that allows a user to specify which HTTP protocols the Presto client is allowed to use.
* Reject @BeforeMethod in Multi-threaded tests.

Cassandra Connector Changes
___________________________
* Add support for Cassandra ``SMALLINT``, ``TINYINT`` and ``DATE`` types and fix tests.

SPI Changes
___________
* Add support for custom query prerequisites to be checked and satisfied through ``QueryPrerequisites`` interface. See :pr:`16073`.
* Add support for custom query prerequisites to be checked and satisfied through ``QueryPrerequisites`` interface. See :pr:`16073`.

Elasticserarch Changes
______________________
* Fix to avoid NullPointerException when there is an unsupported data type column in the Object field.

Hive Changes
____________
* Add support for static AWS credentials in GlueHiveMetastore :doc:`/connector/hive`.

**Contributors**
================

Abhisek Gautam Saikia, Amit Adhikari, Andrew Donley, Andrii Rosa, Ariel Weisberg, Arunachalam Thirupathi, Basar Hamdi Onat, Chen, Chunxu Tang, Darren Fu, Jalpreet Singh Nanda (:imjalpreet), James Petty, James Sun, Julian Zhuoran Zhao, Maria Basmanova, Mayank Garg, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Rongrong Zhong, Sergii Druzkin, Shixuan Fan, Sreeni Viswanadha, Tim Meehan, Venki Korukanti, Zhan Yuan, Zhenxiao Luo, beinan, henneberger, prithvip, v-jizhang
