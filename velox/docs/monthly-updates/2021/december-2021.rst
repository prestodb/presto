********************
December 2021 Update
********************

Documentation
-------------

* Add :doc:`Aggregation-related optimizations </develop/aggregations>` guide.
* Add missing documentation for :func:`json_extract_scalar` function.
* Add documentation for missing functions in :doc:`Mathematical Functions </functions/presto/math>` and :doc:`Comparison Functions </functions/presto/comparison>`.
* Update :doc:`How to add an aggregate function? </develop/aggregate-functions>` guide to reflect changes to the function registry.
* Update :doc:`How to add a scalar function? </develop/scalar-functions>` guide to describe how to write functions with complex type inputs and results.


Core Library
------------

* Add support for partition key aliases to Hive Connector.
* Add support for conversions between Velox string and Arrow string.
* Add :ref:`StreamingAggregation <AggregationNode and StreamingAggregationNode>` operator.
* Fix a bug in HashJoin filter to enable TPC-H query 19.


Presto Functions
----------------

* Add :func:`corr`, :func:`covar_samp`, :func:`covar_pop`, :func:`every` aggregate functions.
* Add :func:`from_base`, :func:`array_position` functions.
* Add support for 'timestamp with timezone' in :func:`to_unixtime` function.


Credits
-------
Aditi Pandit, Alex Hornby, Amit Dutta, Andres Suarez, Andrew Gallagher,
Chao Chen, Cheng Su, Deepak Majeti, Huameng Jiang, Jack Qiao, Kevin Wilfong,
Krishna Pai, Laith Sakka, Marc Fisher, Masha Basmanova, Michael Shang,
Naresh Kumar, Orri Erling, Pedro Eugenio Rocha Pedreira, Sergey Pershin,
Wei He, Wei Zheng, Xavier Deguillard, Yating Zhou, Yuan Chao Chou, Zhenyuan Zhao 
