============
Release 0.70
============

New Machine Learning Functions
------------------------------

We have added a couple new machine learning functions, which can be used
by advanced users familiar with LibSVM. The functions are:
``learn_libsvm_classifier`` and ``learn_libsvm_regressor``. Both take a
parameters string which has the form ``key=value,key=value``

Configurable Initial Split Size for Hive Connector
--------------------------------------------------

There are two new configuration options for the Hive connector,
``hive.max-initial-split-size`` which configures the size of the
initial splits, and ``hive.max-initial-splits`` which configures
the number of initial splits. This can be useful for speeding up small
queries, which would otherwise have low parallelism.

Offline for Presto in Hive Connector
------------------------------------

The Hive connector will now consider all tables with a non-empty value
for the table property ``presto_offline`` to be offline. The value of the
property will be used in the error message.

Presto Verifier
---------------

There is a new project ``presto-verifier`` which can be used to
verify a set of queries against two different clusters. See :ref:`presto_verifier`
for more details.
