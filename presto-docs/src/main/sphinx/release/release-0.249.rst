=============
Release 0.249
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix an error executing index joins when join spilling is enabled.
* Enforce ``max_total_memory_per_node`` on user memory allocation.

Hive Changes
____________
* Fix a bug when S3 access is compromised across s3 enabled hive catalogs (:pr:`15800`).
* Add Support for Hive Connector to work with Hive 3 metastore for basic tables (ACID Tables and tables using constraints are not supported yet) (:pr:`15805`).
* Add Support for Hive Connector to work with Hive 3 metastore for basic tables (ACID Tables and tables using constraints are not supported yet) (:pr:`15805`).

**Contributors**
================

Ajay George, Andrii Rosa, Ariel Weisberg, Arjun Gupta, Arunachalam Thirupathi, Cem Cayiroglu, James Petty, James Sun, John Roll, Ke Wang, Masha Basmanova, Mayank Garg, Neerad Somanchi, Rebecca Schlussel, Rongrong Zhong, Saksham Sachdev, Shixuan Fan, Timothy Meehan, Vic Zhang, Vladimirs Kotovs, Wenlei Xie, agrawalreetika, imjalpreet
