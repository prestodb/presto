=============
Release 0.249
=============

.. warning::
    There is a bug causing memory leak and potentially suboptimal split placement introduced by :pr:`15718`

**Highlights**
==============
* Add support for Hive Connector to work with Hive 3 metastore for basic tables.
* Update Joda-Time to 2.10.8 tzdata 2020d.
* Support computing CRC32 checksum for data exchanges.

**Details**
===========

General Changes
_______________
* Fix an error executing index joins when join spilling is enabled.
* Enforce ``max_total_memory_per_node`` on user memory allocation.
* Update Joda-Time to 2.10.8 tzdata 2020d. Make sure you deploy with a JVM that is using matching tzdata.
* Support computing CRC32 checksum for data exchanges. Disabled by default and can be enabled by setting the ``exchange.checksum-enabled=true`` configuration property.

Hive Changes
____________
* Fix a bug where S3 access is incorrectly cached across S3 enabled hive catalogs (:pr:`15800`).
* Add support for Hive Connector to work with Hive 3 metastore for basic tables (ACID tables and tables using constraints are not yet supported) (:pr:`15805`).
* Add ``new_partition_user_supplied_parameter`` session property which causes all partitions created by a query to have the ``user_supplied`` parameter set to the supplied string in Hive metastore.

Presto on Spark Changes
_______________________
* Add support for distributing broadcast tables using persistent storage, thereby removing the spark driver from the distribution flow. The feature is disabled by default and can be enabled by setting the ``storage_based_broadcast_join_enabled`` session property or the ``spark.storage-based-broadcast-join-storage`` configuration property equal to ``true``.

Security Changes
________________
* Allow read-only configuration access in file-based system access control (:pr:`15715`).

**Contributors**
================

Ajay George, Andrii Rosa, Ariel Weisberg, Arjun Gupta, Arunachalam Thirupathi, Cem Cayiroglu, James Petty, James Sun, John Roll, Ke Wang, Masha Basmanova, Mayank Garg, Neerad Somanchi, Rebecca Schlussel, Rongrong Zhong, Saksham Sachdev, Shixuan Fan, Timothy Meehan, Vic Zhang, Vladimirs Kotovs, Wenlei Xie, agrawalreetika, imjalpreet
