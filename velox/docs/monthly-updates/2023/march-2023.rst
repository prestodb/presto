********************
March 2023 Update
********************

Documentation
=============

 * Add :doc:`Chapter 1  of the Programming Guide <../programming-guide/chapter01>`.
 * Add blog post on `continuous integration and packaging. <https://velox-lib.io/blog/velox-build-experience>`_


Core Library
============

 * Add `printVector` helper function.
 * Add support for `copy_from` in GenericWriter.
 * Add preliminary support for OffProcessExpressionEval operator.
 * Add quota enforcement checks when allocating large chunks of memory.
 * Deprecate `reallocate` Api from memory allocator.
 * Fix aggregate function accumulator's memory alignment.
 * Optimize null aware anit-join when used with filter.
 * Remove usage of CppToType.


Presto Functions
================

 * Add :func:`from_utf8`, :func:`hmac_md5` functions.
 * Add :func:`any_match`, :func:`all_match` and :func:`none_match` functions.
 * Add support for DECIMAL types in :func:`min` and :func:`max` aggregate functions.


Spark Functions
===============

 * Add :spark:func:`trim`, :spark:func:`ltrim`, :spark:func:`rtrim`  function.
 * Add :spark:func:`sha2` function.


Hive Connector
==============

 * Add support for multi-level sub field pruning.
 * Add support for estimating parquet page headers using `ThriftStreamingTransport`.


Performance and Correctness
===========================

 * Add support for custom types in VectorSaver.
 * Add spark fuzzer runs on pull requests.
 * Add try in presto fuzzer runs on pull requests.
 * Add support for serializing SelectivityVector.
 * Add support for serializing query plans.
 * Fix peeling when constant vector smaller than size of inner rows.


Build System
============

 * Add support to run builds using Mac M1 machines on a pull request.
 * Move re2, gtest, gmock and xsimd to use the new dependency resolution system.
 * Refactored dependency resolution system into separate modules.


Python Bindings
===============

 * Add api's to get registered functions and signatures in PyVelox.


Credits
=======

 Aditi Pandit, Barys Skarabahaty, Benjamin Kietzman, Chandrashekhar Kumar Singh, Chen Zhang, Christian Zentgraf, Daniel Munoz, David Tolnay, David Vu, Deepak Majeti, Denis Yaroshevskiy, Ge Gao, Huameng Jiang, Ivan Sadikov, Jacob Wujciak-Jens, Jake Jung, Jeff Palm, Jialiang Tan, Jialing Zhou, Jimmy Lu, Jonathan Kron, Karteek Murthy Samba Murthy, Krishna Pai, Laith Sakka, Masha Basmanova, Matthew William Edwards, Naveen Kumar Mahadevuni, Oguz Ulgen, Open Source Bot, Orri Erling, Patrick Sullivan, Pedro Eugenio Rocha Pedreira, Pramod, Sergey Pershin, Shengxuan Liu, Siva Muthusamy, Srikrishna Gopu, Wei He, Xiaoxuan Meng, Zac, Zhaolong Zhu, cambyzju, dependabot[bot], lingbin, macduan, wuxiaolong26, xiaoxmeng, yangchuan, yingsu00, zhejiangxiaomai, 张政豪
