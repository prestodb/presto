=============
Release 0.266
=============

**Details**
===========

Hive Changes
____________
* Upgrade Parquet to 1.12.0.
* Upgrade Avro to 1.9.2.

Iceberg Changes
_______________
* Add support for caching in Iceberg connector.
* Add support to create Iceberg tables with version1 and version2.
* Upgrade Iceberg to 0.12.0.

Pinot Changes
_____________
* Fix query error when Pinot aggregation query returns mismatched schema.
* Fix query error when Pinot returns empty result.

Verifier Changes
________________
* Add support to skip running checksum queries. This can be enabled with the configuration property ``skip-checksum``.

**Credits**
===========

Ajay George, Arjun Gupta, Arunachalam Thirupathi, Beinan Wang, Deepak Majeti, Gurmeet Singh, James Petty, James Sun, Ke Wang, Masha Basmanova, Nikhil Collooru, Pranjal Shankhdhar, Rongrong Zhong, Sreeni Viswanadha, Swapnil Tailor, Timothy Meehan, Varun Gajjala, Xiang Fu, Ying, Zhan Yuan, Zhenxiao Luo, abhiseksaikia, maobaolong, panyliu, penghuo, sambekar15, tanjialiang, zhaoyulong
