********************
December 2023 Update
********************

Documentation
=============

* Add documentation for :doc:`Runtime Metrics</metrics>`.

Core Library
============

* Add support for ``k range frames`` in ``Window`` operator.
* Add support for aggregations over sorted inputs to ``StreamingAggregation``.
* Add support for TTL in AsyncDataCache and SsdCache. :pr:`6412`
* Add support for TypeSignature Parser using Flex and Bison. This is used to parse function signatures.
* Add support for spilling during the output processing stage of the ``OrderBy`` operator.
* Add support for metrics related to memory arbitration and spilling. :pr:`7940`, :pr:`8025`
* Add config ``max_spill_bytes`` to bound the storage used for spilling. The default value is set to 100GB.
  If it is set to zero, then there is no limit.
* Add ``Status`` class that can be used to carry the success or error state of an operation.
  This is similar to `arrow::Status <https://arrow.apache.org/docs/cpp/api/support.html#_CPPv4N5arrow6StatusE>`_.
* Add :ref:`Expand <ExpandNode>` operator.
* Add config ``max_arbitrary_buffer_size`` to set the maximum size in bytes for a task's buffered
  output when the output is distributed randomly among consumers. The producer drivers are blocked
  when the buffer size exceeds this config.
* Fix reclaiming memory from hash build operators in grouped execution mode. :pr:`8178`
* Fix non-termination of hash join in certain conditions. :pr:`7925`, :pr:`8012`.
* Fix non-termination of the distinct aggregation in certain conditions. :pr:`7968`.
* Fix ``LimitNode`` offset and count values from overflowing.

Presto Functions
================

* Add support for TIMESTAMP WITH TIME ZONE input type to :func:`format_datetime` function.
* Add support for UNKNOWN key type to :func:`map_keys` and :func:`map_values` functions.
* Add support for DECIMAL types to :func:`approx_distinct` aggregate function.
* Add support for ``cast(double|real as varchar)`` to return scientific notation when magnitude
  of the input value is greater than or equal to 10^7, or less than 10^-3.
* Fix :func:`find_first` to return NULL when the input ArrayVector is NULL but
  has non-zero offsets and sizes.
* Fix :func:`find_first` to support input ArrayVectors that are only NULL or empty.
* Fix :func:`find_first` to return NULL for inputs NULL array and 0 index.
* Fix :func:`find_first` to throw an error for inputs empty array and invalid start index.
* Fix :func:`array_sort` to fail gracefully if the specified comparator lambda
  is not supported.
* Fix :func:`transform_keys` to check new keys for NULLs.
* Fix :func:`set_union`, :func:`set_agg` to preserve the order of inputs.
* Fix :func:`map` to produce the correct output if input arrays have NULL rows but with
  invalid offsets and sizes.
* Fix accuracy of the DECIMAL type average computation. :pr:`7944`

Spark Functions
===============

* Add :spark:func:`str_to_map`, :spark:func:`next_day`, :spark:func:`atan2` functions.
* Add support for DECIMAL types to :spark:func:`add` and :spark:func:`subtract` functions.

Hive Connector
==============

* Add support for multiple S3 FileSystems. :pr:`7388`
* Add support to write dictionary and constant encoded vectors to Parquet by flattening them.
* Add support to specify a schema when writing Parquet files. :pr:`6074`
* Add config ``max_split_preload_per_driver`` and remove the ``split_preload_per_driver`` flag.
* Fix memory leak in HdfsBuilder.

Arrow
=====

* Fix exporting an REE array by setting the child name to the canonical name defined in the Arrow spec. :pr:`7802`

Performance and Correctness
===========================

* Add support for lambda functions to ExpressionFuzzer.
* Add ExchangeFuzzer.

Build
=====

* Add support for docker image with Presto.
* Add support for `azure-storage-files-datalake
  <https://github.com/Azure/azure-sdk-for-cpp/releases/tag/azure-storage-files-datalake_12.8.0>`_ version 12.8.0.
* Allow specifying a custom curl version for the cpr library. :pr:`7853`
* Update aws-sdk-cpp version to 1.11.169 (from 1.10.57).

Credits
=======

Aditi Pandit, Amit Dutta, Bikramjeet Vig, Chengcheng Jin, Christian Zentgraf, Daniel Munoz,
Deepak Majeti, Ge Gao, Harvey Hunt, HolyLow, Hongze Zhang, Jacob Wujciak-Jens, Jia, Jia Ke,
Jialiang Tan, Jimmy Lu, Jubin Chheda, Karteekmurthys, Ke, Kevin Wilfong, Krishna Pai,
Krishna-Prasad-P-V, Laith Sakka, Ma-Jian1, Masha Basmanova, Orri Erling, PHILO-HE,
Patrick Sullivan, Pedro Eugenio Rocha Pedreira, Pedro Pedreira, Pramod,Ravi Rahman,
Richard Barnes, Sergey Pershin, Srikrishna Gopu, Wei He, Xiaoxuan Meng, Yangyang Gao,
Yedidya Feldblum, Zac, aditi-pandit, binwei, duanmeng, hengjiang.ly, joey.ljy, rui-mo,
shangjing.cxw, soumyaduriseti, xiaoxmeng, xiyu.zk, xumingming, yan ma, yangchuan ,yingsu00,
zhli, zhli1142015, 高阳阳