=============
Release 0.261
=============

**Highlights**
==============
* Improve performance of :func:`SUM` and :func:`AVG` aggregate functions when used with ``DECIMAL`` type.
* Add column comment to metadata in JDBC based connector
* Add hidden column ``$file_modified_time`` which is the time the file containing the row was last modified.
* Add hidden column ``$file_size`` which is the size of the file containing the row.
* Add support to write data to paths defined by Iceberg's ``LocationProvider`` instead of a hard-coded table root directory.

**Details**
===========

General Changes
_______________
* Fix query failures for queries with shape ``AGG(IF(condition, expr))`` where expr could return exceptions for rows not matching ``condition``. These failures occurred when configuration property ``optimizer.aggregation-if-to-filter-rewrite-enabled`` was enabled.
* Improve performance of :func:`SUM` and :func:`AVG` aggregate functions when used with ``DECIMAL`` type.
* Disable configuration property ``optimizer.aggregation-if-to-filter-rewrite-enabled`` by default.

SPI Changes
___________
* Add ``Identity`` field to ``checkCanSetUser`` for finer granularity of system access control.

Hive Changes
____________
* Add hidden column ``$file_modified_time`` which is the time the file containing the row was last modified.
* Add hidden column ``$file_size`` which is the size of the file containing the row.
* Add CPU usage statistics for file metadata reading to ``RuntimeStats`` table in the coordinator UI and debug mode of presto-cli.

Iceberg Changes
_______________
* Add support to write data to paths defined by Iceberg's ``LocationProvider`` instead of a hard-coded table root directory.

JDBC Changes
____________
* Add column comment to metadata in JDBC based connector.

Prometheus Changes
____________
* Fix startup error by reducing the default value of configuration property ``prometheus.query-chunk-duration`` from ``1d`` to ``10m``.

**Credits**
===========

Andrii Rosa, Arunachalam Thirupathi, Beinan, Dongliang Chen, George Wang, Jack Ye, James Petty, James Sun, Reetika Agrawal, Rongrong Zhong, Shixuan Fan, Sreeni Viswanadha, Swapnil Tailor, Timothy Meehan, Ying, Zhan Yuan, Zhongting Hu, linzebing
