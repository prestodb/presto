********************
January 2023 Update
********************

Core Library
============

* Add support for authoring simple functions with generic results, e.g :func:`subscript`, :func:`array_concat`, :func:`array_intersect`.
* Reject null aware right semi project joins and add support for null-aware left semi join project with filter.
* Fix memory usage tracking in presence of async data cache failures.
* Make memory usage tracker thread safe.

Presto Functions
================

* Add :func:`array_frequency`, :func:`json_parse`, :func:`json_format`, :func:`shuffle`, :func:`sha1`, :func:`spooky_hash_v2_32`, :func:`spooky_hash_v2_64`, :func:`repeat`, :func:`week`, :func:`week_of_year` scalar functions.
* Fix error handling in :func:`least`, :func:`greatest`, :func:`like`, :func:`array_sum`, :func:`array_position`, :func:`regexp_extract`, :func:`regexp_like`, :func:`regexp_extract_all` scalar functions.
* Fix bugs in :func:`array_except`, :func:`array_intersect`, :func:`date_parse`, :func:`bitwise_left_shift`, :func:`bitwise_right_shift` scalar functions.
* Fix crash in :func:`approx_distinct` when passing max standard error value of 0.26 .

Spark Functions
===============

* Add support for DATE inputs to least and greatest functions.
* Fix year function to match Spark's semantics.

Hive Connector
==============

* Fix crashes when reading from an S3 file system.
* Add support for MAP columns in Parquet reader.
* Fix is-null and is-not-null filter pushdown for ARRAY and MAP columns.
* Fix unnecessary copy in the native Parquet reader.

Substrait
=========

* Update Substrait to 0.22.0.

Performance and Correctness
===========================

* Fix multiple bugs in expression evaluation with TRY.
* Revamp function selection mechanism in expression fuzzer. This change will promote more even distribution of test coverage across functions, as the selection of functions will now be based on a list of eligible function names, rather than being based on function signatures, which could lead to an uneven distribution based on factors such as the presence of a templated return type or the number of signatures a function has.
* Add logging of stats in expression fuzzer. This provides visibility into distribution of test coverage among functions and the extent to which they are evaluated on valid inputs.
* Modify fuzzer to skip non-deterministic functions with inputs of complex types.

Portability
============

* Fix Velox build on Mac M1 machines by disabling `lemirebmi2` benchmark.
* Build using GCC 11.3 and LLVM 15 compilers.

Build System
============

* Re-enable continuous benchmarking jobs by moving them to Github Actions.
* Build dependencies in release mode for Ubuntu.

Python Bindings
===============

* Fix memory leak bug when exiting Python.

Credits
=======
Adalto Correia,Aditi Pandit,Amit Dutta,Austin Dickey,Bikramjeet Vig,Chad Austin,Deepak Majeti,Ge Gao,Giuseppe Ottaviano,Gosh Arzumanyan,Huameng Jiang,Ivan Morett,Jacob Wujciak-Jens,Jake Jung,Jialiang Tan,Jimmy Lu,Kevin Wilfong,Krishna Pai,Laith Sakka,Masha Basmanova,Matthew William Edwards,Michael Liu,Michael Shang,Mike Decker,Milosz Linkiewicz,Orri Erling,Patrick Somaru,Pavel Solodovnikov,Pedro Eugenio Rocha Pedreira, Pramod, Qitian Zeng, Randeep Singh, Raúl Cumplido, Sergey Pershin, Siva Muthusamy, Uhyon Chung, Vinti Pandey, Wei He, Weile Wei, Zeyi (Rice) Fan, Zhenyuan Zhao, mwish, shengxuan.liu, tanjialiang, xiaoxmeng, yingsu00, zhejiangxiaomai
