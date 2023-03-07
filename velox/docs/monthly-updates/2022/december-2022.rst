********************
December 2022 Update
********************

Core Library
============

* Simplify the memory management implementation by consolidating the memory
  objects from previously 15 down to 8.

Presto Functions
================

* Add :func:`array_has_duplicates`, :func:`crc32`, :func:`hmac_sha1`,
  :func:`hmac_sha256`, :func:`hmac_sha512`, :func:`json_size` scalar functions.

Spark Functions
===============

* Add :spark:func:`shiftleft` and :spark:func:`shiftright` scalar functions.

Hive Connector
==============

* Add support for IS NULL and IS NOT NULL filter pushdown for maps and arrays
  to DWRF and Parquet readers. (Support is limited to arrays in Parquet reader.)
* Add support for skipping row groups based on remaining filter to DWRF and
  Parquet readers. :pr:`3549`
* Add support for structs to Parquet reader.

Substrait
=========

* Add support for OrderBy/TopN/Limit plan nodes.

Performance and Correctness
===========================

* Add nightly job to run Expression Fuzzer using Spark SQL functions.
* Add nightly job to run Join Fuzzer.
* Add basic support for window queries to Aggregation Fuzzer.
* Add support for CAST and SWITCH special forms to Expression Fuzzer.

Build System
============

* Add nightly job to build Docker image for Prestissimo using latest Velox.
* Add nightly job to build Docker images using Ubuntu and CentOS.
* Moved boost from pre-installed system dependency to automatically
  installed via CMake resolve_dependency.

Python Bindings
===============

* Add PyVelox Python package with bindings to convert Python lists to Velox
  vectors, construct and evaluate expressions.

Credits
=======

Abhash Jain, Aditi Pandit, Arpit Porwal, Austin Dickey, Bikramjeet Vig, Ge Gao,
Huameng Jiang, Jacob Wujciak-Jens, Jimmy Lu, Karteek Murthy Samba Murthy,
Kevin Wilfong, KevinyhZou, Kk Pulla, Krishna Pai, Laith Sakka, Masha Basmanova,
Orri Erling, Pedro Eugenio Rocha Pedreira, Pramod, Pucheng Yang, Rama Malladi,
Richard Barnes, Rui Mo, Sasha Krassovsky, Sergey Pershin, Wei He, Xiaoxuan Meng,
ZJie1, frankzfli, lingbin, macduan, xyz, yingsu00, zhixingheyi-tian
