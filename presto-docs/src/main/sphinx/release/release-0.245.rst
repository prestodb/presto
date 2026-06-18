=============
Release 0.245
=============

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured (:pr:`15501`).

.. warning::
    There is a bug causing failure when reading ORC files with ``ARRAY``/``MAP``/``ROW`` of ``VARCHAR`` columns using the selective stream readers for some corner cases (:pr:`15549`)

**Highlights**
==============
* New ``enum_key`` UDF to get the key for an enum value.
* Configurable query result compression.

**Details**
===========

General Changes
_______________
* Fix wrong results issue for queries with multiple lambda expressions that differ only in array/map/row constants that have the same length (:issue:`15424`).
* Fix serialization bug causing maps with string-valued enum keys to be displayed as illegible strings in Presto CLI.
* Fix parsing logic for prepared statements to be consistent with regular statements.
* Improve memory usage for fragment result caching.
* Add support for disabling query result compression via client and server-side configuration properties. Clients can disable compressed responses using the ``--disable-compression`` flag or ``disableCompression`` driver property. Compression can be disabled server-wide by using the configuration property: ``query-results.compression-enabled=false``
* Add support in affinity scheduler to enable cache for bucketed table scan that has remote source.
* Add ``enum_key`` to get the key corresponding to an enum value: `ENUM_KEY(EnumType) -> VARCHAR`.

Hive Connector Changes
______________________
* Improve parquet metadata reader by preloading data and reducing the number of reads (:pr:`15421`).

SPI Changes
____________
* Add ``getSchema`` to ``ConnectorSession``.

**Contributors**
================

Andrii Rosa, Beinan Wang, Bhavani Hari, Daniel Ohayon, Dong Shi, Ge Gao, James Petty, James Sun, Ke Wang, Leiqing Cai, Masha Basmanova, Mayank Garg, Nikhil Collooru, Rebecca Schlussel, Rongrong Zhong, Saksham Sachdev, Shixuan Fan, Timothy Meehan, Vic Zhang, Wenlei Xie, Ying Su, Zhenxiao Luo
