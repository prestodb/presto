********************
January 2024 Update
********************

Documentation
=============

* Add documentation about :doc:`Hash Table </develop/hash-table>`.
* Add documentation about :doc:`memory management </develop/memory>`.

Core Library
============

* Add metrics to track time spent in memory arbitration. :pr:`8497`, :pr:`8482`
* Add metric to track average buffer time for exchange. :pr:`8534`
* Optimize count(distinct x) when x is of complex type. :pr:`8560`
* Optimize latency for exchange that uses arbitrary buffer. :pr:`8532`, :pr:`8480`
* Optimize MallocAllocator to reduce lock contention. :pr:`8477`
* Fix aggregation over all-null keys with ignoreNullKeys = true. :pr:`8422`
* Fix race condition in task completion that caused `Output buffers for task not found` failures. :pr:`8357`
* Fix evaluation of CAST expression under TRY. :pr:`8365`
* Fix `FlatVector<StringView>::copy` for vectors with more than 2GB of data. :pr:`8516`
* Fix crash in `FlatVector<bool>::ensureWritable`. :pr:`8450`
* Fix interaction of spilling and yielding in Hash Join operator. :pr:`8520`
* Fix rawInputPositions metrics in Exchange operator. :pr:`8370`

Presto Functions
================

* Add :func:`from_ieee754_64`, :func:`multimap_from_entries`, :func:`ngrams` functions.
* Add support for VARBINARY inputs to :func:`reverse` function.
* Add support for arrays of complex types to :func:`array_min` and :func:`array_max` functions.
* Add support for casting DOUBLE and VARCHAR as DECIMAL.
* Add support for UNKNOWN key to :func:`map_agg` function.
* Add support for timezone offsets to :func:`timezone_hour` and :func:`timezone_minute` functions. :pr:`8269`
* Optimize cast from JSON by using simdjson. :pr:`8216`
* Fix handling of timestamps with timezone in :func:`date_diff` function. :pr:`8540`
* Fix :func:`json_parse` for inputs with very large numbers. :pr:`8455`
* Fix kAtLeastN and kExactlyN fast paths in LIKE for inputs with multi-byte characters. :pr:`8150`
* Fix :func:`approx_distinct` aggregate function for TIMESTAMP inputs. :pr:`8164`
* Fix :func:`min` and :func:`max` when used in Window operator. :pr:`8311`

Spark Functions
===============

* Add :spark:func:`from_unixtime`, :spark:func:`find_in_set`, :spark:func:`get_timestamp`,
  :spark:func:`hour`, :spark:func:`hex`, :spark:func:`isnan`, :spark:func:`replace` functions.
* Add support for TINYINT and SMALLINT inputs to :spark:func:`date_add` and :spark:func:`date_sub` functions.
* Add support for casting DOUBLE and VARCHAR as DECIMAL.
* Fix crash in :spark:func:`conv` function. :pr:`8046`

Hive Connector
==============

* Fix crash in Parquet reader when processing empty row groups. :pr:`8000`
* Fix data sink to avoid writing partition columns to files. :pr:`8089`

Performance and Correctness
===========================

* Add support for aggregations over distinct inputs to AggregationFuzzer.
* Reduce memory usage of histogram metrics. :pr:`8458`
* Add Join Fuzzer run to CI that runs on each PR.
* Add Aggregation Fuzzer run using Presto as source of truth to experimental CI.

Build System
============

* Upgrade folly to v2023.12.04.00 (from v2022.11.14.00).
* Upgrade fmt to 10.1.1 (from 8.0.1).

Credits
=======

Amit Dutta, Benwei Shi, Bikramjeet Vig, Chen Zhang, Chengcheng Jin, Christian
Zentgraf, Deepak Majeti, Ge Gao, Hongze Zhang, Jacob Wujciak-Jens, Jia Ke,
Jialiang Tan, Jimmy Lu, Ke, Kevin Wilfong, Krishna Pai, Laith Sakka, Lu Niu,
Ma, Rong, Masha Basmanova, Mike Lui, Orri Erling, PHILO-HE, Pedro Eugenio
Rocha Pedreira, Pratik Joseph Dabre, Ravi Rahman, Richard Barnes, Schierbeck,
Cody, Sergey Pershin, Sitao Lv, Taras Galkovskyi, Wei He, Yedidya Feldblum,
Yuan Zhou, Yuping Fan, Zac Wen, aditi-pandit, binwei, duanmeng, hengjiang.ly,
icejoywoo, lingbin, mwish, rui-mo, wypb, xiaoxmeng, xumingming, yangchuan,
yingsu00, youxiduo, yuling.sh, zhli1142015, zky.zhoukeyong, zwangsheng
