******************
August 2023 Update
******************


Documentation
=============

* Add documentation about :doc:`adaptive disabling of partial aggregation </develop/aggregations>`.
* Add documentation about :doc:`Aggregate::toIntermediate() API </develop/aggregate-functions>`.
* Add documentation about :doc:`Aggregate::trackRowSize </develop/aggregate-functions>`.
* Add documentation about :doc:`Connectors </develop/connectors>`.


Core Library
============

* Add checks for verifying internal state of a vector. :pr:`5885`
* Add support for remote functions. :pr:`6104`
* Add support for accessing sub-fields by index. :pr:`6253`
* Add support for DECIMAL in UnsafeRowSerializer.
* Add support for any type in element_at and subscript operator. :pr:`5973`
* Add support to cast DECIMAL to BOOL.
* Add support to cast DECIMAL to REAL and Integral types.
* Add support for DECIMAL in IN predicate.
* Add support for REAL in IN predicate.
* Enhance layout of hash tables using an interleaved layout to improve cache hits.
* Enhance COALESCE and SWITCH special forms to check all inputs are of same type.
* Fix exception handling in TRY_CAST for JSON inputs. :pr:`6116`


Presto Functions
================

* Add :func:`array_remove`, :func:`date_format`, :func:`format_datetime`, :func:`last_day_of_month`, :func:`split_to_map`, :func:`trim` functions.
* Add :func:`gamma_cdf`, :func:`poisson_cdf`, :func:`laplace_cdf` .
* Add :func:`wilson_interval_lower`, :func:`wilson_interval_upper` functions.
* Add support for week option in :func:`date_trunc`.
* Add support for legacy ignoreNulls option to :func:`array_agg`.
* Optimize :func:`array_constructor` function.


Spark Functions
===============

* Add :spark:func:`date_add`, :spark:func:`date_sub`, :spark:func:`week_of_year`, :spark:func:`dayofyear`, :spark:func:`dayofmonth`, :spark:func:`dayofweek` functions.
* Add :spark:func:`pmod` function.
* Add :spark:func:`max_by`, :spark:func:`min_by` aggregate functions.
* Add :spark:func:`row_number` window function.
* Add support for seed in :spark:func:`hash` function.


Hive Connector
==============

* Add support for writing to `Google Cloud Storage <https://cloud.google.com/storage>`_. :pr:`5685`
* Fix writing to HDFS as part of CREATE TABLE AS SELECT (CTAS) query. :pr:`5663`


Performance and Correctness
===========================

* Add benchmark for spilling. :pr:`6071`
* Add support for compression in spilling. :pr:`5904`
* Optimize single partition spilling.
* Add longer fuzzer runs when pull request has changes to certain files and paths. :pr:`6009`
* Enable more feature flags when running Spark expression fuzzer.
* Enhance ExchangeClient to maintain a cap on ExchangeQueue size.
* Enhance Aggregation Fuzzer to test "abandon partial aggregation" code paths.
* Enhance Aggregation Fuzzer to generate random complex types as input. :pr:`6312`
* Enhance Aggregation Fuzzer to create plans that exercise TableScan node. :pr:`6298`
* Prevent out of memory errors when building hash table in HashBuild operator.


Build Systems
=============

* Run document generation job on PR merge to main.
* Schedule nightly fuzzer runs to use Github Actions.


Credits
=======

Alexander Yermolovich, Amit Dutta, Ann Rose Benny, Arun D. Panicker, Ashwin Krishna Kumar, Austin Dickey, Bikramjeet Vig, Chengcheng Jin, Christian Zentgraf, Daniel Munoz, David Tolnay, Deepak Majeti, Ebe Janchivdorj, Ge Gao, Giuseppe Ottaviano, Harsha Rastogi, Hongze Zhang, Jacob Wujciak-Jens, Jia Ke, Jialiang Tan, Jimmy Lu, Karteek Murthy Samba Murthy, Karteekmurthys, Ke, Kevin Wilfong, Krishna Pai, Laith Sakka, Luca Niccolini, Ma-Jian1, Mack Ward, Mahadevuni Naveen Kumar, Masha Basmanova, Mike Lui, Nick Terrell, Open Source Bot, Orri Erling, Patrick Sullivan, Pedro Eugenio Rocha Pedreira, Pedro Pedreira, Pramod, Pranjal Shankhdhar, Richard Barnes, Rong Ma, Sandino Flores, Sanjiban Sengupta, Shiyu Gan, Wei He, Zac, Zhe Wan, aditi-pandit, duanmeng, ericyuliu, generatedunixname89002005287564, generatedunixname89002005325676, jackylee-ch, leesf, root, rui-mo, wangxinshuo.db, wypb, xiaoxmeng, yingsu00, yiweiHeOSS, zhejiangxiaomai, 陈旭