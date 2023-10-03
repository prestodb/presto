*********************
September 2023 Update
*********************


Documentation
=============

* Add :doc:`Chapter 2 of programming guide </programming-guide/chapter02>`
* Add blog post about `learning from optimizing try_cast <https://velox-lib.io/blog/optimize-try_cast.>`_


Core Library
============

* Add BaseVector::containsNullAt API :pr:`6515`
* Add ContainerRowSerde::compareWithNulls API :pr:`6419`
* Add Operator::initialize API to handle customized operation init :pr:`6422`
* Add Pacific/Kanton, Europe/Kyiv, America/Ciudad_Juarez time zones :pr:`6670`
* Add support for Array/Map/RowVector::copyRanges from UNKNOWN source :pr:`6607`
* Add support for HUGEINT in function signatures :pr:`6799`
* Add support for prioritizing productive sources in Exchange :pr:`6459`
* Add Type::isOrderable() and Type::isComparable() APIs :pr:`6770`


Presto Functions
================

* Add overlapped string support in :func:`strpos` function. :pr:`6410`
* Add :func:`to_ieee754_64` function :pr:`6445`
* Add support for all input types to IN predicate :pr:`6513`
* Add support for UNKNOWN in :func:`map_from_entries` function :pr:`6353`
* Add support for VARBINARY input to :func:`length` function :pr:`6795`
* Add CAST DECIMAL to VARCHAR :pr:`6210`
* Add CAST TIMESTAMP WITH TIME ZONE to and from TIMESTAMP :pr:`6529`
* Add :func:`bitwise_xor_agg` aggregate function :pr:`6705`
* Add :func:`geometric_mean` aggregate function :pr:`6678`
* Add :func:`reduce_agg` aggregate function :pr:`6482`
* Add support for UNKNOWN type in :func:`arbitrary` aggregate function :pr:`6557`
* Add support for complex types to compare argument of :func:`min_by` and :func:`max_by` :pr:`6605`
* Fix :func:`array_remove` null handling :pr:`6424`
* Fix handling null string in :func:`json_extract` :pr:`6439`
* Fix :func:`url_extract_path` function :pr:`6657`
* Fix :func:`min`, :func:`max`, :func:`min_by`, and :func:`max_by` aggregates when input contains nested nulls :pr:`6723`
* Fix :func:`set_union` aggregate :pr:`6800`
* Fix :func:`lead` and :func:`lag` window function for int64 offset :pr:`6463`
* Optimize exception handling in SIMD JSON functions :pr:`6776`


Spark Functions
===============

* Add comparison Spark functions :pr:`5569`
* Add support for :spark:func:`rand` function with seed specified :pr:`6112`
* Add support for all patterns in CAST VARCHAR to DATE :pr:`5844`


Hive Connector
==============

* Add fast path of FlatMap column reader for scalar types :pr:`6507`
* Add support for writing query results to S3 :pr:`6021`
* Add support for writing sorted bucketed hive table :pr:`6142`
* Pass TableScan output type to Parquet column reader :pr:`6404`
* Reduce peak memory usage by roughly half in HiveDataSource :pr:`6601`


Performance and Correctness
===========================

* Fix CPU time reporting by attributing LazyVector loading time to TableScan :pr:`6558`
* Optimize TRY_CAST performance by reducing throwing :pr:`5913`
* Optimize large chunk memory allocation in stream arena :pr:`6547`
* Optimize spill performance by buffering spill data write :pr:`6509`
* Tune memory arbitration and spilling for Velox batch use cases :pr:`6789`


Build Systems
=============

* Install Azure SDK needed to support Azure Storage ABFS Connector :pr:`6418`


Credits
=======

Ankita Victor, Artem Gelun, Austin Dickey, Bikramjeet Vig, Chengcheng Jin,
Christian Zentgraf, Deepak Majeti, Ge Gao, George Wang, Jacob Wujciak-Jens,
Jia Ke, Jialiang Tan, Jimmy Lu, Jubin Chheda, Karteekmurthys, Ke, Kevin Wilfong,
Krishna Pai, Laith Sakka, Luca Niccolini, Mahadevuni Naveen Kumar, Manav Avlani,
Manikandan Somasundaram, Masha Basmanova, Muir Manders, Orri Erling, PHILO-HE,
Patrick Stuedi, Patrick Sullivan, Pedro Pedreira, Pramod, Pratik Joseph Dabre,
Rong Ma, Sergey Pershin, Shanyue Wan, Shiyu Gan, Srikrishna Gopu, Surabhi Pandit,
Wei He, Yangyang Gao, Zac, Zac Wen, aditi-pandit, duanmeng, ericyuliu,
generatedunixname89002005232357, joey.ljy, lingbin, mayan, rui-mo, usurai, wypb,
xiaodou, xiaoxmeng, xumingming, yingsu00, zhli1142015, 陈旭, 高阳阳
