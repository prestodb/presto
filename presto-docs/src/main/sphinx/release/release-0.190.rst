=============
Release 0.190
=============

General Changes
---------------

* Fix correctness issue for :func:`array_min` and :func:`array_max` when arrays contain ``NaN``.
* Fix planning failure for queries involving ``GROUPING`` that require implicit coercions
  in expressions containing aggregate functions.
* Fix potential workload imbalance when using topology-aware scheduling.
* Fix performance regression for queries containing ``DISTINCT`` aggregates over the same column.
* Fix a memory leak that occurs on workers.
* Improve error handling when a ``HAVING`` clause contains window functions.
* Avoid unnecessary data redistribution when writing when the target table has
  the same partition property as the data being written.
* Ignore case when sorting the output of ``SHOW FUNCTIONS``.
* Improve rendering of the ``BingTile`` type.
* The :func:`approx_distinct` function now supports a standard error
  in the range of ``[0.0040625, 0.26000]``.
* Add support for ``ORDER BY`` in aggregation functions.
* Add dictionary processing for joins which can improve join performance up to 50%.
  This optimization can be disabled using the ``dictionary-processing-joins-enabled``
  config property or the ``dictionary_processing_join`` session property.
* Add support for casting to ``INTERVAL`` types.
* Add :func:`ST_Buffer` geospatial function.
* Allow treating decimal literals as values of the ``DECIMAL`` type rather than ``DOUBLE``.
  This behavior can be enabled by setting the ``parse-decimal-literals-as-double``
  config property or the ``parse_decimal_literals_as_double`` session property to ``false``.
* Add JMX counter to track the number of submitted queries.

Resource Groups Changes
-----------------------

* Add priority column to the DB resource group selectors.
* Add exact match source selector to the DB resource group selectors.

CLI Changes
-----------

* Add support for setting client tags.

JDBC Driver Changes
-------------------

* Add ``getPeakMemoryBytes()`` to ``QueryStats``.

Accumulo Changes
----------------

* Improve table scan parallelism.

Hive Changes
------------

* Fix query failures for the file-based metastore implementation when partition
  column values contain a colon.
* Improve performance for writing to bucketed tables when the data being written
  is already partitioned appropriately (e.g., the output is from a bucketed join).
* Add config property ``hive.max-outstanding-splits-size`` for the maximum
  amount of memory used to buffer splits for a single table scan. Additionally,
  the default value is substantially higher than the previous hard-coded limit,
  which can prevent certain queries from failing.

Thrift Connector Changes
------------------------

* Make Thrift retry configurable.
* Add JMX counters for Thrift requests.

SPI Changes
-----------

* Remove the ``RecordSink`` interface, which was difficult to use
  correctly and had no advantages over the ``PageSink`` interface.

.. note::

    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector that uses the ``RecordSink`` interface,
    you will need to update your code before deploying this release.
