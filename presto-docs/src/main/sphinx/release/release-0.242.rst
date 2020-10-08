=============
Release 0.242
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix compiler error in LambdaBytecodeGenerator when CSE is enabled and multiple SQL functions contain lambda expressions.
* Fix possible permanent stuck queries when memory limit exceeded during shuffle.
* Add config `experimental.spiller.task-spilling-strategy` for choosing different spilling strategy to use.
* Add session property ``query_max_output_bytes`` and configuration property ``query.max-output-bytes`` to control how much data a query can output.
* Add support for fragment result caching. When enabled, if the same plan fragment and same connector split hit the same worker, engine would directly fetch result from cache and skip computation. Currently only partial aggregation is supported. Cache could be enabled by setting ``fragment-result-cache.enabled`` to ``true`` and tuned by other configs started with ``fragment-result-cache``. Query could use fragment result cache by setting config ``experimental.fragment-result-caching-enabled`` or session property ``fragment_result_caching_enabled`` to ``true``.
* Parquet files written by parquet-avro library that uses TIMESTAMP_MICROS as the OriginalType to represent timestamp can now be queried by presto.

Verifier Changes
________________
* Fix an issue in determinism analysis would indicate failing due to data being changed while the data is not changed.
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
* Upgrade Esri to [2.2.4](https://github.com/Esri/geometry-api-java/releases/tag/v2.2.4).  This includes two fixes for bug (https://github.com/Esri/geometry-api-java/issues/266 and https://github.com/Esri/geometry-api-java/issues/247) that were seen in production.

Hive Changes
____________
* Fix a bug where DWRF encryption would fail for large uncompressed column values.
* Fix a bug where non-Presto readers could not read encrypted DWRF files written by Presto if the encryption group listed columns out of order.
* Fix a performance regression for String field handling in GenericHiveRecordCursor when the SerDe does not provide an efficient Writable implementation.
* Improve split loading efficiency by only using as many threads as are required.
* Add support for file renaming for Hive connector. This can be enabled with ``hive.file_renaming_enabled`` configuration property.

**Credits**
===========

Andrii Rosa, Ariel Weisberg, Bin Fan, Daniel Ohayon, Dharak Kharod, James Gill, James Petty, James Sun, Ke Wang, Leiqing Cai, Maria Basmanova, Masha Basmanova, Mayank Garg, Nikhil Collooru, Palash Goel, Rebecca Schlussel, Rongrong Zhong, Saksham Sachdev, Sanjay Sundaresan, Saumitra Shahapure, Shixuan Fan, Sreeni Viswanadha, Tim Meehan, Timothy Meehan, Vic Zhang, Vivek, Weidong Duan, Wenlei Xie, Xiang Fu, Ying Su, Yuya Ebihara, Zhenxiao Luo, ankit0811, asdf2014, fornaix
