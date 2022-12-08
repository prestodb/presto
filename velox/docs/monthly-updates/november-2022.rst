********************
November 2022 Update
********************

Documentation
=============

* Document :doc:`spilling <../develop/spilling>`.
* Document :doc:`configuration properties <../configs>`.
* Document :doc:`expression and aggregation fuzzers <../develop/testing/fuzzer>`.

Core Library
============

* Add support for semi join project. :pr:`3068`
* Add support for using aggregate functions in window operator.
* Add support for unbounded preceding, unbounded following, and current row frame
  bounds in range mode to window operator.
* Add support for ROWS frame mode to window operator.
* Add support for reporting 'live' runtime statistics while Task is running.

Presto Functions
================

* Add :func:`strrpos` and :func:`truncate` scalar functions.
* Add :func:`ntile` window function.
* Add support for BOOLEAN and INTERVAL DAY TO SECOND inputs to :func:`min`,
  :func:`max` and :func:`histogram` aggregate functions.
* Add support for BOOLEAN, TIMESTAMP, DATE and INTERVAL DAY TO SECOND input types
  to :func:`arbitrary` aggregate function.

Spark Functions
===============

* Add bitwise_and, bitwise_or, xxhash64 functions.

Performance and Correctness
===========================

* Add aggregation and join fuzzers.
* Add functions with variadic signatures to expression fuzzer.
* Add COALESCE and IF special forms to expression fuzzer.
* Add support for Lazy Vectors to expression fuzzer.
* Add support to expression fuzzer for saving repro data for crashes. :pr:`3074`

Credits
=======

Aditi Pandit, Amit Dutta, Austin Dickey, Behnam Robatmili, Bikramjeet Vig,
Chad Austin, Chengcheng Jin, Ge Gao, Gosh Arzumanyan, Huameng Jiang,
Jacob Wujciak-Jens, Jake Jung, Jia Ke, Jimmy Lu, Karteek Murthy Samba Murthy,
Kevin Wilfong, KevinyhZou, Krishna Pai, Laith Sakka, Luca Niccolini, Masha Basmanova,
Orri Erling, PHILO-HE, Pedro Eugenio Rocha Pedreira, Pramod,
Qitian Zeng, Raúl Cumplido, Rong Ma, Sasha Krassovsky, Sergey Pershin,
Shengxuan Liu, Wei He, Zhenyuan Zhao, lingbin, macduan, tanjialiang,
xiaoxmeng, yingsu00, zhixingheyi-tian
