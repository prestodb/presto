=============
Release 0.178
=============

General Changes
---------------

* Fix various memory accounting bugs, which reduces the likelihood of full GCs/OOMs.
* Fix a regression that causes queries that use the keyword "stats" to fail to parse.
* Fix an issue where a query does not get cleaned up on the coordinator after query failure.
* Add ability to cast to ``JSON`` from ``REAL``, ``TINYINT`` or ``SMALLINT``.
* Add support for ``GROUPING`` operation to :ref:`complex grouping operations<complex_grouping_operations>`.
* Add support for correlated subqueries in ``IN`` predicates.
* Add :func:`to_ieee754_32` and :func:`to_ieee754_64` functions.

Hive Changes
------------

* Fix high CPU usage due to schema caching when reading Avro files.
* Preserve decompression error causes when decoding ORC files.

Memory Connector Changes
------------------------

* Fix a bug that prevented creating empty tables.

SPI Changes
-----------

* Make environment available to resource group configuration managers.
* Add additional performance statistics to query completion event.
