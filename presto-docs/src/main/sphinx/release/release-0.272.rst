=============
Release 0.272
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix parquet data page offset calculation in parquet writer.
* Add a new JDBC driver parameter - timeZoneID :doc:`/installation/jdbc` :issue:`16680`.
* Add secure_random() and secure_random(lower, upper) with uniform distribution.
* Add support to use Parquet page-level statistics to skip pages.
* Bumped Alluxio client from `2.7.0` to `2.7.3`.
* Enable cost based optimization by default.
* Ensure Parameters are in proper order for queries having WITH clause.
* Follow the SQL standard in cast from double or real to varchar.
* Preserve quotedness for Identifier when formatting Query.

Mongodb Connector Changes
_________________________
* Fix the spelling of the write concern option ``JOURNAL_SAFE`` for the property ``mongodb.write-concern``.

Hive Changes
____________
* Introduce a flag "hive.parquet-column-index-filter-enabled" to turn on/off this feature. By default it is off.
* Support metadata-based listing and bootstrap for Hudi tables.
* Upgrade Hudi version to 0.10.1.

Iceberg Changes
_______________
* Support concurrent insertion from the same presto cluster or mutiple presto clusters which sharing the same metastore.

Pinot Changes
_____________
* Support querying Pinot JSON type.

**Credits**
===========

Alan Xu, Amit Dutta, Ariel Weisberg, Arjun Gupta, Arunachalam Thirupathi, Beinan Wang, Chen, Chunxu Tang, Darren Fu, David N Perkins, George Wang, Guy Moore, Harsha Rastogi, James Petty, James Sun, Josh Soref, Ke Wang, Maksim Dmitriyevich Podkorytov, Masha Basmanova, Mayank Garg, Neerad Somanchi, Nikhil Collooru, Pranjal Shankhdhar, Rebecca Schlussel, Rongrong Zhong, Ruslan Mardugalliamov, Sagar Sumit, Sergey Smirnov, Sergii Druzkin, Swapnil Tailor, Tim Meehan, Todd Gao, Varun Gajjala, Xiang Fu, Xinli shang, Ying, Zhenxiao Luo, ahouzheng, guojianhua, mengdilin, panyliu, v-jizhang, zhangyanbing
