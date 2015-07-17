============
Release 0.77
============

Parametric Types
----------------
Presto now has a framework for implementing parametric types and functions.
Support for :ref:`array_type` and :ref:`map_type` types has been added, including the element accessor
operator ``[]``, and new :doc:`/functions/array`.

Streaming Index Joins
---------------------
Index joins will now switch to use a key-by-key streaming join if index
results fail to fit in the allocated index memory space.

Distributed Joins
-----------------
Joins where both tables are distributed are now supported. This allows larger tables to be joined,
and can be enabled with the ``distributed-joins-enabled`` flag. It may perform worse than the existing
broadcast join implementation because it requires redistributing both tables.
This feature is still experimental, and should be used with caution.

Hive Changes
------------
* Handle spurious ``AbortedException`` when closing S3 input streams
* Add support for ORC, DWRF and Parquet in Hive
* Add support for ``DATE`` type in Hive
* Fix performance regression in Hive when reading ``VARCHAR`` columns

Kafka Changes
-------------
* Fix Kafka handling of default port
* Add support for Kafka messages with a null key

General Changes
---------------
* Fix race condition in scheduler that could cause queries to hang
* Add ConnectorPageSource which is a more efficient interface for column-oriented sources
* Add support for string partition keys in Cassandra
* Add support for variable arity functions
* Add support for :func:`count` for all types
* Fix bug in HashAggregation that could cause the operator to go in an infinite loop
