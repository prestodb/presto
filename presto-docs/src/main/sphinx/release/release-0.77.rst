============
Release 0.77
============

Parametric Types
----------------
Presto now has a framework for implementing parametric types and functions.
Support for :ref:`array_type` and :ref:`map_type` types has been added, including the element accessor
operator ``[]``, and new :ref:`array_functions`.

Hive Changes
------------

* Handle spurious ``AbortedException`` when closing S3 input streams

Distributed Joins
-----------------

Joins where both tables are distributed are now supported. This allows larger tables to be joined,
and can be enabled with the ``distributed-joins-enabled`` flag. It may perform worse than the existing
broadcast join implementation because it requires redistributing both tables.
This feature is still experimental, and should be used with caution.

General Changes
---------------

* Cassandra plugin now supports string partition keys
* Add support for variable arity functions
* :func:`count` now works for all types
