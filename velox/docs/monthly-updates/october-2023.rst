*******************
October 2023 Update
*******************

Documentation
=============

* Add documentation for simple UDAF interface.
* Add blog post about `reduce_agg <https://velox-lib.io/blog/reduce-agg>`_ lambda aggregate function.
* Extend documentation for datetime Presto functions to explain handling of :ref:`time zones <presto-time-zones>`.
* Extend documentation for :func:`reduce_agg` Presto lambda aggregate function.

Core Library
============

* Add spill support to Window, RowNumber and TopNRowNumber operators.
* Add spill support after receiving all input to HashAggregation operator. :pr:`6903`
* Add spill stats to the output of printPlanWithStats.
* Add logic to adaptively abandon partial TopNRowNumber if cardinality reduction is not sufficient. :pr:`7195`
* Add optimized version of Window operator for the case when inputs are already partitioned and sorted. :pr:`5437`
* Add support for order-able and comparable arguments to function signatures.
* Add support for order-able and comparable arguments to the Simple Function interface. :pr:`7293`
* Fix Unnest operator to honor `preferred_output_batch_rows` configuration property and avoid producing huge vectors. :pr:`7051`

Presto Functions
================

* Add :func:`find_first` and :func:`find_first_index` scalar lambda functions.
* Add :func:`any_match`, :func:`all_match`, :func:`none_match` scalar lambda functions.
* Add :func:`all_keys_match`, :func:`any_keys_match`, :func:`any_values_match`,
  :func:`no_keys_match`, :func:`no_values_match` scalar lambda functions.
* Add :func:`remove_nulls` scalar function.
* Add :func:`ends_with` and :func:`starts_with` scalar functions.
* Add :func:`to_ieee754_32` scalar function.
* Add support for non-constant patterns and escape characters to :func:`like` function. :pr:`6917`
* Add support for BOOLEAN inputs to :func:`least` and :func:`greatest` scalar functions.
* Add support for INTEGER inputs to :func:`poisson_cdf` and :func:`binomial_cdf` scalar functions.
* Add support for maps with keys of UNKNOWN type in :func:`map_filter` scalar lambda function.
* Add support for REAL inputs to :func:`geometric_mean` aggregate function.
* Add support for floating point keys to :func:`map_union_sum` aggregate function.
* Add support for CAST to and from complex types with nested JSON values. :pr:`7256`
* Fix 1ms-off issue in :func:`from_unixtime` scalar function. :pr:`7047`
* Fix :func:`array_min` and :func:`array_max` for floating point numbers to match Presto. :pr:`7128`
* Fix :func:`checksum` aggregate function. :pr:`6910`
* Fix :func:`array_sort` and :func:`contains` scalar functions to reject inputs with nested nulls.
* Fix :func:`map_agg`, :func:`set_agg`, :func:`min_by` and :func:`max_by` aggregate functions to
  reject inputs with nested nulls.
* Fix :func:`array_sort` and :func:`array_sort_desc` to restrict inputs to order-able types. :pr:`6928`
* Fix :func:`min`, :func:`min_by`, :func:`max`, :func:`max_by` aggregate functions to restrict inputs to order-able types. :pr:`7232`
* Fix CAST(VARCHAR as JSON) for Unicode characters. :pr:`7119`
* Fix CAST(JSON as ROW) to use case-insensitive match for keys. :pr:`7016`

Spark Functions
===============

* Add :spark:func:`array_min`, :spark:func:`array_max`, :spark:func:`add_months`,
  :spark:func:`conv`, :spark:func:`substring_index`, :spark:func:`datediff` scalar functions.
* Add support for DECIMAL inputs to :spark:func:`multiply` and :spark:func:`divide`.
* Fix :spark:func:`sum` aggregate function for BIGINT inputs to allow overflow.

Hive Connector
==============

* Add support for reading from Azure Storage. :pr:`6675`

Performance and Correctness
===========================

* Optimize spilling by switching to `gfx::timsort <https://github.com/timsort/cpp-TimSort>`_ (from std::sort). :pr:`6745`.
* Add support for disabling caching in expression evaluation to reduce memory usage via `enable_expression_evaluation_cache` configuration property. :pr:`6898`
* Add support for validating output of every operator via `debug.validate_output_from_operators` configuration property. :pr:`6687`
* Add support for order-able function arguments to the Fuzzer. :pr:`6950`
* Fix edge cases in datetime processing during daylight saving transition. :pr:`7011`
* Fix comparisons of complex types values using floating point numbers in the RowContainer. :pr:`5833`
* Fix window aggregations for empty frames. :pr:`6872`
* Fix GroupID operator with duplicate grouping keys in the output. :pr:`6738`
* Fix global grouping set aggregations for empty inputs. :pr:`7112`
* Fix aggregation function framework to require raw input types for all aggregates to avoid confusion and incorrect results. :pr:`7037`

Build Systems
=============

* Add support for Conda Environments. :pr:`6282`

Credits
=======

Alex, Alex Hornby, Amit Dutta, Ann Rose Benny, Bikramjeet Vig, Chengcheng Jin,
Christian Zentgraf, Cody Ohlsen, Daniel Munoz, David Tolnay, Deepak Majeti,
Genevieve (Genna) Helsel, Huameng (Michael) Jiang, Jacob Wujciak-Jens, Jaihari
Loganathan, Jason Sylka, Jia Ke, Jialiang Tan, Jimmy Lu, John Elliott, Jubin
Chheda, Karteekmurthys, Ke, Kevin Wilfong, Krishna Pai, Krishna-Prasad-P-V,
Laith Sakka, Ma-Jian1, Mahadevuni Naveen Kumar, Mark Shroyer, Masha Basmanova,
Orri Erling, PHILO-HE, Patrick Sullivan, Pedro Eugenio Rocha Pedreira, Pramod,
Prasoon Telang, Pratik Joseph Dabre, Pratyush Verma, Rong Ma, Sergey Pershin,
Wei He, Zac, aditi-pandit, dependabot[bot], duanmeng, joey.ljy, lingbin,
rrando901, rui-mo, usurai, wypb, xiaoxmeng, xumingming, yan ma, yangchuan,
yingsu00, zhejiangxiaomai, 高阳阳
