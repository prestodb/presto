*****************
April 2024 Update
*****************

Documentation
=============

* Document operations on decimals for :doc:`Presto </functions/presto/decimal>`
  and :doc:`Spark </functions/spark/decimal>`.
* Document spill write stats. :pr:`9326`

Core Library
============

* Fix bugs in Window operator. :pr:`9476`, :pr:`9271`, :pr:`9257`

Presto Functions
================

* Add :func:`word_stem` and :func:`to_iso8601` scalar functions.
* Add support for DECIMAL inputs to :func:`arbitrary`, :func:`min` and :func:`max` aggregate functions.
* Fix :func:`json_extract` for paths with wildcards.

Spark Functions
===============

* Add :spark:func:`array_size`, :spark:func:`flatten`, :spark:func:`year_of_week` scalar functions.
* Add :spark:func:`collect_list` and :spark:func:`regr_replacement` aggregate functions.

Hive Connector
==============

* Add support for storing decimal as integer in Parquet writer.
* Add hive.s3.connect-timeout, hive.s3.socket-timeout and hive.s3.max-connections configs. :pr:`9472`
* Fix complex type handing in Parquet reader. :pr:`9187`
* Fix DWRF reader to skip null map keys.

Performance and Correctness
===========================

* Add aggregation and window fuzzer runs to every PR.
* Add nightly run of window fuzzer.
* Add check for aggregate function signature changes to every PR.
* Add biased aggregation fuzzer run for newly added aggregate functions to every PR.

Build System
============

* Add nightly job to track build metrics.

Credits
=======

Andres Suarez, Andrii Rosa, Ankita Victor, Ashwin Krishna Kumar, Bikramjeet Vig,
Christian Zentgraf, Daniel Munoz, David McKnight, Deepak Majeti, Hengzhi Chen,
Huameng (Michael) Jiang, Jacob Wujciak-Jens, Jeongseok Lee, Jialiang Tan, Jimmy
Lu, Karteekmurthys, Ke, Kevin Wilfong, Krishna Pai, Lu Niu, Ludovic Henry, Ma,
Rong, Mahadevuni Naveen Kumar, Masha Basmanova, Mike Lui, Minhan Cao, PHILO-HE,
Pedro Eugenio Rocha Pedreira, Pedro Pedreira, Pramod, Qian Sun, Richard Barnes,
Sergey Pershin, Shabab Ayub, Tengfei Huang, Terry Wang, Wei He, Weitao Wan,
Wills Feng, Yang Zhang, Yihong Wang, Yoav Helfman, Zac Wen, Zhenyuan Zhao,
aditi-pandit, chliang, cindyyyang, duanmeng, jay.narale, joey.ljy, mohsaka,
rui-mo, svm1, willsfeng, wutiangan, wypb, xiaoxmeng, yingsu00, zhli1142015
