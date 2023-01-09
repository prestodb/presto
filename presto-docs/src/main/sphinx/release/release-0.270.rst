=============
Release 0.270
=============

**Details**
===========

General Changes
_______________
* Fix error classification when an invalid timezone is passed as a parameter to :func:`from_unixtime`.
* Improve performance of ``DISTINCT LIMIT N`` queries for N <= 10000. This can be enabled with the session property ``hash_based_distinct_limit_enabled``
  or the configuration property ``hash-based-distinct-limit-enabled`` and the limit can be adjusted by using the session property ``hash_based_distinct_limit_threshold``
  or the configuration property ``hash-based-distinct-limit-threshold``.
* Add :func:`last_day_of_month` UDF to return the last day of the month.
* Add dynamic filtering support for right join.
* Add support for any expression for dynamic filtering probe side.
* Add new optimizer rule to simplify expressions like ``cardinality(map_keys(m))`` into ``cardinality((m))``. Same for :func:`map_values` function.
* Add support for the following primitive types to Avro decoder: ``TINYINT``, ``SMALLINT``, ``INTEGER`` and ``REAL``.

Hive Connector Changes
____________
* Remove the configuration property ``hive.parquet.fail-on-corrupted-statistics`` and the session property ``parquet_fail_with_corrupted_statistics``.

Iceberg Connector Changes
_______________
* Remove the configuration property ``iceberg.native-mode``. Use ``iceberg.catalog.type`` instead.

Pinot Connector Changes
_____________
* Add support for Pinot ``TIMESTAMP`` and ``JSON`` types.
* Add support for Pinot version 0.9.3.

**Credits**
===========

Amit Dutta, Arunachalam Thirupathi, Beinan, Chunxu Tang, James Petty, James Sun, Nikolay Laptev, Pedro Sereno, Pranjal Shankhdhar, Rongrong Zhong, Sreeni Viswanadha, Timothy Meehan, Xiang Fu, Yang Yang, Zhenxiao Luo, chuxiao, ericyuliu, wangjingdong, zhangyanbing
