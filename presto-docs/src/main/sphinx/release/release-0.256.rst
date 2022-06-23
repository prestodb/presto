=============
Release 0.256
=============

**Details**
===========

General Changes
_______________
* Add support for lambda expressions in ``PREPARE`` statements.
* Add configuration property ``experimental.distinct-aggregation-spill-enabled`` and session property ``distinct_aggregation_spill_enabled`` to disable spilling for distinct aggregations.
* Add configuration property ``experimental.order-by-aggregation-spill-enabled`` and session property ``order_by_aggregation_spill_enabled`` to disable spilling for order by aggregations.
* Add double quotes around the column domain values in the text query plan. Literal double quotes in values will be escaped with a backslash (``ab"c`` -> ``ab\"c``).
* Add function :func:`array_frequency`.
* Add support for :func:`zip` to take up to 7 arguments.

Web UI Changes
______________
* Add ``Cumulative Total Memory`` to the ``Resource Utilization Summary`` table on the ``Query Details`` page.

SPI Changes
___________
* Add ``cumulativeTotalMemory`` to ``QueryStatistics``.

**Contributors**
================

Abhisek Gautam Saikia, Amit Dutta, Andrii Rosa, Ariel Weisberg, Arunachalam Thirupathi, Beinan, Chunxu Tang, Dong Shi, Ge Gao, Grace Xin, Huameng Jiang, James Petty, James Sun, Jennifer Zhou, Jim Apple, Julian Zhuoran Zhao, Kyle B Campbell, Maria Basmanova, Marilyn Beck, Rebecca Schlussel, Rongrong Zhong, Ryan Guo, Seba Villalobos, Shixuan Fan, Tim Meehan, Vic Zhang, Xiang Fu, Zhan Yuan, tanjialiang
