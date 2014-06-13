============
Release 0.70
============

Hive
----

There are two new configuration options for the Hive connector,
``hive.max-initial-split-size`` which configures the size of the
initial splits, and ``hive.max-initial-splits`` which configures
the number of initial splits. This can be useful for speeding up small
queries, which would otherwise have low parallelism.


The Hive connector will now consider all tables with a non-empty value
for the table property ``presto_offline`` to be offline. The value of the
property will be used in the error message.

We have added support for for ``DROP TABLE`` in the hive connector.  By default
this feature is not enabled.  To enable it set
``hive.allow-drop-table=true`` in your hive catalog properties file.

Presto Verifier
---------------

There is a new project ``presto-verifier`` which can be used to
verify a set of queries against two different clusters. See :ref:`presto_verifier`
for more details.

Connector Improvements
----------------------

Connectors can now add hidden columns to a table. Hidden columns are not
displayed in ``DESCRIBE TABLE`` or ``information_schema``, and are not
considered for ``SELECT *``.  As an example, we have added a hidden
``row_number`` column to the ``tpch`` connector.

Presto contains an extensive test suite to verify the correctness.  This test
suite has been extracted into the ``presto-test`` module for use during
connector development. For an example, see ``TestRaptorDistributedQueries``.

New Machine Learning Functions
------------------------------

We have added a couple new machine learning functions, which can be used
by advanced users familiar with LibSVM. The functions are:
``learn_libsvm_classifier`` and ``learn_libsvm_regressor``. Both take a
parameters string which has the form ``key=value,key=value``

General Changes
---------------

* The DUAL synthetic table is no longer supported. As an alternative, please
  write your queries without a FROM clause or use the VALUES syntax.
* New comparison functions: :func:`greatest` and :func:`least`
* New window function: :func:`first_value`, :func:`last_value`, and
  :func:`nth_value`
* We have added a config option to disable falling back to the interpreter when
  expressions fail to be compiled to bytecode. To set this option, add 
  ``compiler.interpreter-enabled=false`` to config.properties
* Dates are now implicitly coerced to TIMESTAMP and TIMESTAMP WITH TIME ZONE by
  setting the hour/minute/seconds to 0 with respect to the session timezone
* Minor performance optimization when planning queries over tables with tens of
  thousands of partitions or more
* Fixed a bug when planning ORDER BY … LIMIT queries which could result in
  duplicate and unordered results under rare conditions
* Hive connector no longer recurses into HDFS directories when enumerating
  splits
* Reduce the size of stats collected from tasks, which dramatically reduces
  garbage generation and improves coordinator stability
* Fix compiler cache for expressions

