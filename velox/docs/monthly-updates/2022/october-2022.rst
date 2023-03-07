*******************
October 2022 Update
*******************

Core Library
============

* Add regular (not null aware) :doc:`Anti Join </develop/anti-join>`. :pr:`2906`
* Add spilling support for Hash Join.
* Add query configuration properties to configure spilling: `spill_enabled`,
  `aggregation_spill_enabled`, `join_spill_enabled`, `order_by_spill_enabled`,
  `aggregation_spill_memory_threshold`, `order_by_spill_memory_threshold`.
* Fix signatures of simple functions using custom types. :pr:`2965`

Presto Functions
================

* Add :func:`truncate` function.
* Add :func:`map_zip_with` function.
* Add :func:`nth_value` and :func:`cume_dist` window functions.
* Optimize :func:`cardinality` function.
* Fix :func:`from_unixtime` for NaN input. :pr:`2936`
* Fix :func:`array_sum` for constant input. :pr:`2932`
* Fix :func:`array_intersect` and :func:`array_except` for dictionary encoded or
  null input. :pr:`2959`
* Fix :func:`arrays_overlap`, :func:`array_intersect`, and :func:`array_except`
  when one argument is a constant array with dictionary-encoded elements. :pr:`2954`
* Fix `sum(real)` aggregate function.
* Fix CAST to JSON when invoked inside of a TRY.
* Fix lambda functions when invoked inside of a TRY.

Hive Connector
==============

* Add support for DECIMAL types to Parquet reader.
* Add support for S3 FileSystem compatible schemes `s3a` and `oss`. :pr:`2410`

Performance and Correctness
===========================

* Add mechanism to reproduce Fuzzer failures reliably: run ExpressionFuzzer
  with `--repro_persist_path` flag to save repro files, then run ExpressionRunner
  to reproduce.
* Add support for functions that take or return complex type values to Fuzzer.
  Use `--velox_fuzzer_enable_complex_types` flag to enable.
* Add support for LazyVector to VectorSaver and VectorFuzzer.

Debugging Experience
====================

* Add printIndices(indices) and printNulls(nulls) functions. :pr:`2723` and :pr:`2721`

Credits
=======

Adalto Correia, Aditi Pandit, Amit Dutta, Austin Dickey, Bikramjeet Vig, Chad
Austin, Deepak Majeti, Ge Gao, Giuseppe Ottaviano, Gosh Arzumanyan, Huameng
Jiang, Ivan Morett, Jacob Wujciak-Jens, Jake Jung, Jialiang Tan, Jimmy Lu,
Kevin Wilfong, Krishna Pai, Laith Sakka, Masha Basmanova, Michael Liu, Michael
Shang, Mike Decker, Milosz Linkiewicz, Open Source Bot, Orri Erling, Patrick
Somaru, Pavel Solodovnikov, Pedro Eugenio Rocha Pedreira, Pedro Pedreira,
Pramod, Qitian Zeng, Randeep Singh, Raúl Cumplido, Sergey Pershin, Uhyon Chung,
Vinti Pandey, Wei He, Weile Wei, Zeyi (Rice) Fan, Zhenyuan Zhao, mwish,
shengxuan.liu, tanjialiang, xiaoxmeng, yingsu00, zhejiangxiaomai