********************
February 2023 Update
********************

Documentation
=============

 * Add documentation for :doc:`Spark functions <../spark_functions>`.
 * Add documentation for :doc:`PyVelox <../bindings/python/README_generated_pyvelox>`.


Core Library
============

 * Add support for DECIMAL types in DecodedVector.
 * Add support for k PRECEDING/FOLLOWING window frames in ROWS mode.
 * Enhance BloomFilter so that it can be used in aggregate functions.
 * Replace ArrayBuilder class with ArrayWriter.


Presto Functions
================

 * Add :func:`array_normalize` function.
 * Add support for DECIMAL inputs to :func:`least` and :func:`greatest` functions.
 * Fix :func:`sum` and :func:`avg` functions for DECIMAL inputs.
 * Fix :func:`approx_distinct` for streaming use cases. :pr:`3965`


Spark Functions
===============

 * Add :spark:func:`sha1` function.


Hive Connector
==============

 * Add support for single-level subfield pruning. :pr:`3949`
 * Add support for BOOLEAN and DATE type in native Parquet reader.
 * Add options to open and prepare file splits asynchronously.
 * Fix reading of VARBINARY columns in Parquet reader.


Substrait
=========

 * Update Substrait to 0.23.0.
 * Add support for `emit <https://substrait.io/tutorial/sql_to_substrait/#column-selection-and-emit>`_
 * Add support for DATE type.


Arrow
=====

 * Add :ref:`ArrowStream operator`.
 * Add support for DATE type.


Performance and Correctness
===========================

 * Add concurrent memory allocation benchmark.
 * Add support for CAST and TRY special forms to Fuzzer.
 * Add support to favorably select certain functions.
 * Add support to generate repro files for window fuzzers.
 * Add support for nested expression re-use.
 * Add support for DECIMAL types to VectorFuzzer.
 * Add FuzzerConnector to source randomly generated data. :pr:`4094`
 * Add GeneratorSpec to support generating vectors with customized data,  nulls and encoding.
 * Fix bugs in CAST found by Fuzzer.
 * Fix memory leaks in Prestissimo use cases.


Build System
============

 * Add support to build `PyVelox wheels for Linux and MacOS <https://github.com/facebookincubator/velox/actions/workflows/build_pyvelox.yml>`_.
 * Add support to compare benchmarks during Conbench CI runs.


Credits
=======

Aditi Pandit, Bikramjeet Vig, ChenZhang, Chengcheng Jin, Christy Lee-Eusman, Deepak Majeti, Ge Gao, Hazem Ibrahim Nada, Hualong Gervais, Huameng Jiang, Ivan Sadikov, Jacob Wujciak-Jens, Jimmy Lu, Karteek Murthy Samba Murthy, Krishna Pai, Laith Sakka, Manikandan Somasundaram, Mark Shroyer, Masha Basmanova, Michael Shang, Open Source Bot, Orri Erling, Pedro Eugenio Rocha Pedreira, Pramod, Sergey Pershin, Wei He, Weiguo Chen, Xianda Ke, Xuedong Luan, ZJie1, joey.ljy, rui-mo, vibhatha, xiaoxmeng, yangchuan, yoha.zy, zhejiangxiaomai, zky.zhoukeyong, 张政豪
