*****************
March 2024 Update
*****************

Documentation
=============

* Document `design philosophy <https://velox-lib.io/docs/community/design-philosophy/>`_
* Document custom input generators and verifiers supported in the Aggregation Fuzzer.
* Document runtime stats reported by the HashTable. :pr:`9255`
* Document usage of generic types in Simple Function API. :pr:`9084`

Core Library
============

* Add prefix-sort for fixed width sorting keys.
* Add null behavior and determinism scalar function metadata to the registry. :pr:`9209`
* Add order-sensitive aggregate function metadata to the registry. :pr:`9050`
* Add support for DECIMAL type to Simple Function API. :pr:`9096`
* Add support for lambda functions (reduce_agg) to StreamingAggregation.
* Deprecate threshold based spilling in Aggregation and OrderBy.
* Optimize Exchange protocol used by Presto for latency. :pr:`8845`

Presto Functions
================

* Add :func:`day`, :func:`from_ieee754_32`, :func:`hamming_distance`, :func:`map_normalize`,
  :func:`map_top_n` scalar functions.
* Add support for DECIMAL input type to :func:`floor` function.
* Add support for timestamp +/- IntervalYearMonth.
* Add :func:`regr_avgx`, :func:`regr_avgy`, :func:`regr_count`, :func:`regr_r2`,
  :func:`regr_sxx`, :func:`regr_sxy`, and :func:`regr_syy` aggregation functions.

Spark Functions
===============

* Add :spark:func:`array_remove`, :spark:func:`bit_length`, :spark:func:`bitwise_xor`,
  :spark:func:`bitwise_not`, :spark:func:`make_ym_interval`, :spark:func:`from_utc_timestamp`,
  :spark:func:`to_utc_timestamp`, :spark:func:`make_timestamp`, :spark:func:`map_subset`,
  :spark:func:`unhex`, :spark:func:`unix_date`, :spark:func:`uuid` functions.
* Add :spark:func:`regexp_replace` function.
* Add :spark:func:`monotonically_increasing_id`, :spark:func:`spark_partition_id` functions.
* Add :spark:func:`kurtosis` and :spark:func:`skewness` aggregation functions.
* Add support for DECIMAL inputs to :spark:func:`sum` aggregation function.
* Add CAST(real as decimal).
* Add configuration property 'spark.partition_id'.

Hive Connector
==============

* Add support for S3 client no_proxy CIDR expression. :pr:`9160`
* Add support for synthetic columns '$file_size' and '$file_modified_time'.
* Optimize reading a small sample of rows. :pr:`8920`.
* Fix Parquet reader for files with different encodings across row groups. :pr:`9129`

Performance and Correctness
===========================

* Add nightly run of Aggregation fuzzer using Presto as source of truth.
* Add nightly run of Exchange fuzzer.
* Add utility to randomly trigger OOMs and integrate it into Aggregation and Join fuzzers.
* Add group execution mode to Join fuzzer.
* Add support for random frame clause generation to Window fuzzer.
* Add custom input generator for map_union_sum Presto aggregation function.
* Add custom result verifier for arbitrary Presto aggregation function.

Credits
=======

8dukongjian, Amit Dutta, Ankita Victor, Bikramjeet Vig, Christian Zentgraf,
Daniel Munoz, Deepak Majeti, Ge Gao, InitialZJ, Jacob Wujciak-Jens, Jake Jung,
Jialiang Tan, Jimmy Lu, Karteekmurthys, Kevin Wilfong, Krishna Pai, Ma,  Rong,
Mahadevuni Naveen Kumar, Marcus D. Hanwell, Masha Basmanova, Nicholas Ormrod,
Nick Terrell, Orri Erling, PHILO-HE, Patrick Sullivan, Pedro Pedreira, Pramod,
Pratik Joseph Dabre, Qian Sun, Richard Barnes, Sandino Flores, Schierbeck,
Cody, Sergey Pershin, Ubuntu, Wei He, Yang Zhang, Zac Wen, aditi-pandit,
duanmeng, f0rest9999, hengjiang.ly, joey.ljy, lingbin, mwish, rexan, rui-mo,
willsfeng, wypb, xiaodai1002, xiaoxmeng, xumingming, youxiduo, yuling.sh,
zhli1142015, zky.zhoukeyong
