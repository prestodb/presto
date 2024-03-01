=============
Release 0.258
=============

**Details**
===========

General Changes
_______________
* Fix a bug in SQL functions where a query could fail with a compiler error if the function has a lambda variable with the same name as a column or function input.
* Add Cauchy distribution CDF :func:`cauchy_cdf` and inverse CDF :func:`inverse_cauchy_cdf` functions.
* Add SQL functions :func:`array_dupes` and :func:`array_as_dupes`.
* Add additional details to memory exceeded error messages to simplify debugging. Disabled by default. Can be enabled by setting the ``verbose_exceeded_memory_limit_errors_enabled`` session property to ``true``.
* Support dynamic filtering with comparison operators. Can be enabled by setting the ``enable-dynamic-filtering`` property to ``true``. Disabled by default.

Cassandra Changes
________________
* Improve query performance by caching metadata to avoid extra remote calls to the Cassandra server.

Elasticsearch Changes
_____________________
* Fix incorrect pushdown of ``IS NULL`` predicate in Elasticsearch.

Hive Changes
____________
* Fix a bug in Parquet dereference pushdown that caused inconsistent query results in certain cases.
* Add support for allowing to match columns between table and partition schemas by names for HUDI tables. This is enabled when configuration property ``hive.parquet.use-column-names`` or the hive catalog session property ``parquet_use_column_names`` is set to ``true``. By default they are mapped by index.

Iceberg Changes
_________________________
* Add support for ORC files.

SPI Changes
___________
* Add ``getClientTags`` to ``ConnectorSession``.

**Credits**
===========

Andrii Rosa, Beinan, Chen, Grace Xin, Huameng Jiang, Jalpreet Singh Nanda (:imjalpreet), James Petty, James Sun, Jeremy Craig Sawruk, Julian Zhuoran Zhao, Junyi Huang, Marilyn Beck, Neerad Somanchi, Pranjal Shankhdhar, Roman Zeyde, Rongrong Zhong, Sergey Pershin, Shixuan Fan, Tim Meehan, Venki Korukanti, Xiang Fu, Zhan Yuan, Zhenxiao Luo, derektbrown, jiachen, lijieliang, 护城
