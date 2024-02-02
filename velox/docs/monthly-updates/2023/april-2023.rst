********************
April 2023 Update
********************


Documentation
=============

 * Add :doc:`documentation on UnsafeRow serialization </develop/unsaferow>`.


Core Library
============

 * Add support to adaptively disable non-productive partial aggregations. :pr:`4588`
 * Add support for peeling past a loaded lazy layer of a vector.
 * Add support for companion function generation for aggregations.
 * Add support to suppress errors for unused arguments of null-propagating functions.
 * Add non-thread-safe memory usage tracking for cpu-sensitive use cases.
 * Add support for memory arbitrator framework and shared memory arbitrator.



Presto Functions
================

 * Add :func:`date`,   :func:`random()`,  :func:`sequence`,  :func:`from_base64url` functions.
 * Add :func:`from_big_endian_32`,  :func:`from_big_endian_64`,  :func:`to_big_endian_32`,  :func:`to_big_endian_64` functions.
 * Add :func:`timezone_minute`,   :func:`timezone_hour` functions.
 * Add :func:`first_value` and :func:`last_value` window functions.
 * Add support for DECIMAL :func:`abs` and :func:`negate` functions.
 * Implemented DECIMAL comparison functions as vector functions.


Spark Functions
===============

 * Add :spark:func:`substring`,   :spark:func:`might_contain` functions.
 * Add :spark:func:`unix_timestamp`,  :spark:func:`to_unix_timestamp` functions.
 * Add :spark:func:`bit_xor` aggregate function.
 * Fix :spark:func:`map` function.


Hive Connector
==============

 * Add support for pushing down deletes into reader.
 * Add support to pushdown partition key when also used as a join key.
 * Fix map key pruning when all keys are filtered out.


Performance and Correctness
===========================

 * Enable coalesce in expression fuzzer.
 * Fix handling of errors during evaluation of switch expressions.
 * Fix memory leak in expression eval.
 * Make decoding of ConstantVector<bool> safer.


Build System
============

 * Add support to alert on changes in function signatures. :pr:`4461`
 * Add support to run conbench benchmarks without docker.
 * Replace find_library with find_package for more robust dependency handling.


Python Bindings
===============

 * Add bindings for constant and dictionary encoded vectors.


Credits
=======

 Aditi Pandit, Ann Rose Benny, Bikramjeet Vig, Chad Austin, Chen Zhang, Chengcheng Jin, Deepak Majeti, GOPU-Krishna-S, Ge Gao, Ivan Sadikov, Jacob Wujciak-Jens, Jialiang Tan, Jimmy Lu, Karteek Murthy Samba Murthy, Ke, Krishna Pai, Laith Sakka, Leo Yan, Mark Shroyer, Masha Basmanova, Open Source Bot, Orri Erling, PHILO-HE, Pedro Eugenio Rocha Pedreira, Pramod, Pranjal Shankhdhar, Pratyush Verma, Sasha Krassovsky, Sergey Pershin, Victor Zverovich, Wei He, Xiaoxuan Meng, Zac, akashsha1, ashokku202, joey.ljy, psbell-meta, usurai, xiaodou, xiaoxmeng, yangchuan, zhejiangxiaomai, zky.zhoukeyong
