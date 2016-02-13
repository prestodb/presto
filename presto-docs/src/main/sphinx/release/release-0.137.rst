=============
Release 0.137
=============

General Changes
---------------

* Fix ``current_date`` to return correct results for all time zones.
* Fix invalid plans when scalar subqueries use ``GROUP BY``, ``DISTINCT`` or ``JOIN``.
* Do not allow creating views with a column type of ``UNKNOWN``.
* Improve expression optimizer to remove some redundant operations.
* Add :func:`bit_count`, :func:`bitwise_not`, :func:`bitwise_and`,
  :func:`bitwise_or`, and :func:`bitwise_xor` functions.
* Add :func:`approx_distinct` aggregation support for ``VARBINARY`` input.
* Add create time to query detail page in UI.
* Add support for ``VARCHAR(length)`` type.
* Track per-stage peak memory usage.
* Allow using double input for :func:`approx_percentile` with an array of
  percentiles.
* Add API to JDBC driver to track query progress.

Hive Changes
------------

* Do not allow inserting into tables when the Hive type does not match
  the Presto type. Previously, Presto would insert data that did not
  match the table or partition type and that data could not be read by
  Hive. For example, Presto would write files containing ``BIGINT``
  data for a Hive column type of ``INT``.
* Add validation to :doc:`/sql/create-table` and :doc:`/sql/create-table-as`
  to check that partition keys are the last columns in the table and in the same
  order as the table properties.
* Remove ``retention_days`` table property. This property is not used by Hive.
* Fix Parquet decoding of ``MAP`` containing a null value.
* Add support for accessing ORC columns by name. By default, columns in ORC
  files are accessed by their ordinal position in the Hive table definition.
  To access columns based on the names recorded in the ORC file, set
  ``hive.orc.use-column-names=true`` in your Hive catalog properties file.
