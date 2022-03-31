********************
March 2022 Update
********************

Documentation
-------------

* Document :doc:`printPlanWithStats <../develop/debugging/print-plan-with-stats>` debugging tool.

Core Library
------------

* Add support for UNNEST WITH ORDINALITY for single array or map.
* Add support for filter to INNER and LEFT merge joins.
* Add support for FULL OUTER hash join.
* Add Substrait-to-Velox plan conversion.
* Add exec::resolveAggregateFunction API to resolve aggregate function final and intermediate types based on function name and input types.
* Add KLL sketch implementation to be used to implement :func:`approx_percentile` Presto function with accuracy parameter. :pr:`1247`
* Add GTest as a submodule and upgrade it to 1.11.0 (from 1.10.0).
* Reduce dependencies for VELOX_BUILD_MINIMAL build by removing GTest, GMock and compression libraries. :pr:`1292`
* Simplify simple functions API to allow to provide void call method for functions that never return null for non-null inputs.
* Optimize LocalMerge operator. :pr:`1264`
* Optimize PartitionedOutput and Exchange operators by removing unnecessary data copy. :pr:`1127`
* Optimize BaseVector::compare() for a pair of flat or a pair of constant vectors or scalar types. :pr:`1317`
* Optimize BaseVector::copy() for flat vectors of scalar type. :pr:`1316`
* Optimize aggregation by enabling pushdown into table scan in more cases. :pr:`1188`
* Optimize simple functions that never return null for non-null input for 70% perf boost. :pr:`1152`
* Fix bucket-by-bucket (grouped) execution of join queries.
* Fix crashes and correctness bugs in joins and aggregations.

Presto Functions
----------------

* Add support for CAST(x as JSON).
* Add :func:`arrays_overlap` function.
* Extend :func:`hour` function to support TIMESTAMP WITH TIME ZONE inputs.
* Fix :func:`parse_datetime` and :func:`to_unixtime` semantics. :pr:`1277`
* Fix :func:`approx_distinct` to return 0 instead of null with all inputs are null.

Performance and Correctness Testing
-----------------------------------

* Add linux-benchmarks-basic CircleCI job to run micro benchmarks on each PR. Initial set of benchmarks covers SelectivityVector, DecodedVector, simple comparisons and conjuncts.
* Add TPC-H benchmark with support for q1, q6 and q18.
* Add support for approximate verification of REAL and DOUBLE values returned by test queries.
* Add support for ORDER BY SQL clauses to PlanBuilder::localMerge() and PlanBuilder::orderBy().

Debugging Experience
--------------------

* Improve error messages in expression evaluation by including the expression being evaluated. :pr:`1138`
* Improve error messages in CAST expression by including the to and from types. :pr:`1150`
* Add printPlanWithStats function to print query plan with runtime statistics.
* Add SelectivityVector::toString() method.
* Improve ConstantExpr::toString() to include constant value.
* Add runtime statistic "loadedToValueHook" to track number of values processed via aggregation pushdown into scan.

Credits
-------

Aditi Pandit, Amit Dutta, Amlan Nayak, Chad Austin, Chao Chen, David Greenberg,
Deepak Majeti, Dimitri Bouche, Gilson Takaasi Gil, Hanqi Wu, Huameng Jiang,
Jialiang Tan, im Meyering, Jimmy Lu, Karteek Murthy Samba Murthy, Kevin
Wilfong, Krishna Pai, Laith Sakka, Liang Tao, MJ Deng, Masha Basmanova, Orri
Erling, Paula Lahera, Pedro Eugenio Rocha Pedreira, Pradeep Garigipati, Richard
Barnes, Rui Mo, Sagar Mittal, Sergey Pershin, Simon Marlow, Siva Muthusamy,
Sridhar Anumandla, Victor Zverovich, Wei He, Wenlei Xie, Yoav Helfman, Yuan
Chao Chou, Zhenyuan Zhao, tanjialiang