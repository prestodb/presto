=============
Release 0.242
=============

.. warning::
    There is a bug in LambdaDefinitionExpression canonicalization introduced since 0.238. For more details, go to :issue:`15424`.

.. warning::
    There is a bug causing failure at startup if function namespace manager is enabled and Thrift is not configured. Fixed in :pr:`15501`.

**Highlights**
==============
* Add configuration property ``experimental.spiller.task-spilling-strategy`` for choosing different spilling strategy to use.
* Add support for fragment result caching.
* Add support for partition versioning.
* Add support for caching HiveMetastore calls at a more granular level.

**Details**
===========

General Changes
_______________
* Fix compiler error in ``LambdaBytecodeGenerator`` when common sub-expression optimization is enabled and multiple SQL functions contain lambda expressions.
* Fix possible permanently stucked queries when memory limit is exceeded during shuffle.
* Fix a bug where ``IS DISTINCT FROM NULL`` produces wrong results for columns with null values.
* Add configuration property ``experimental.spiller.task-spilling-strategy`` for choosing different spilling strategy to use.
* Add session property ``query_max_output_bytes`` and configuration property ``query.max-output-bytes`` to control how much data a query can output.
* Add support for fragment result caching. This can be enabled by ``fragment-result-cache.enabled`` and ``experimental.fragment-result-caching-enabled``.

Hive Changes
____________
* Fix a bug where DWRF encryption would fail for large uncompressed column values.
* Fix a bug where non-Presto readers could not read encrypted DWRF files written by Presto if the encryption group listed columns out of order.
* Fix a performance regression for String field handling in GenericHiveRecordCursor when the SerDe does not provide an efficient Writable implementation.
* Improve split loading efficiency by only using as many threads as are required.
* Add support for file renaming for Hive connector. This can be enabled with the ``hive.file_renaming_enabled`` configuration property.
* Add support to query parquet files written by parquet-avro library that uses ``TIMESTAMP_MICROS`` as the ``OriginalType`` to represent timestamp.
* Add support for partition versioning. This can be enabled with ``hive.partition-versioning-enabled`` configuration property
* Add support for caching HiveMetastore calls at a more granular level. This can be specified by the ``hive.metastore-cache-scope`` configuration property.

Pinot Changes
_____________
* Add configuration property ``pinot.pushdown-topn-broker-queries`` to allow pushing down TOPN queries to Pinot.

Cassandra Change
________________
* Add ``SMALLINT``, ``TINYINT``, and ``DATE`` type support.

Verifier Changes
________________
* Fix an issue in determinism analysis which would indicate failing due to data being changed while the data is not changed.
* Improve JSON plan comparison for explain verification. (:pr:`15198`).
* Add application-name config to override source passed in ClientInfo.
* Add support to fetch the list of queries to be verified by running a Presto query.
* Add support to run explain verification. (:pr:`15101`). This can be enabled by configuration property ``explain``.

SPI Changes
___________
* Add ``getIdentifier`` to ``ConnectorTableLayoutHandle``. Layout identifier is used in fragment result caching to construct canonical plan.
* Add ``getSplitIdentifier`` to ``ConnectorSplit``. Split identifier is used in fragment result caching to identify if splits are identical.

Geospatial Changes
__________________
* Upgrade Esri to [2.2.4](https://github.com/Esri/geometry-api-java/releases/tag/v2.2.4). This includes two fixes for bug (https://github.com/Esri/geometry-api-java/issues/266 and https://github.com/Esri/geometry-api-java/issues/247) that were seen in production.

**Contributors**
================

Andrii Rosa, Ariel Weisberg, Bin Fan, Daniel Ohayon, Dharak Kharod, James Gill, James Petty, James Sun, Ke Wang, Leiqing Cai, Masha Basmanova, Mayank Garg, Nikhil Collooru, Palash Goel, Rebecca Schlussel, Rongrong Zhong, Saksham Sachdev, Sanjay Sundaresan, Saumitra Shahapure, Shixuan Fan, Sreeni Viswanadha, Tim Meehan, Vic Zhang, Vivek, Weidong Duan, Wenlei Xie, Xiang Fu, Ying Su, Yuya Ebihara, Zhenxiao Luo, ankit0811, asdf2014, fornaix
