=============
Release 0.181
=============

General Changes
---------------

* Fix query failure and memory usage tracking when query contains
  :func:`transform_keys` or :func:`transform_values`.
* Prevent ``CREATE TABLE IF NOT EXISTS`` queries from ever failing with *"Table already exists"*.
* Fix query failure when ``ORDER BY`` expressions reference columns that are used in
  the ``GROUP BY`` clause by their fully-qualified name.
* Fix excessive GC overhead caused by large arrays and maps containing ``VARCHAR`` elements.
* Improve error handling when passing too many arguments to various
  functions or operators that take a variable number of arguments.
* Improve performance of ``count(*)`` aggregations over subqueries with known
  constant cardinality.
* Add ``VERBOSE`` option for :doc:`/sql/explain-analyze` that provides additional
  low-level details about query performance.
* Add per-task distribution information to the output of ``EXPLAIN ANALYZE``.
* Add support for ``DROP COLUMN`` in :doc:`/sql/alter-table`.
* Change local scheduler to prevent starvation of long running queries
  when the cluster is under constant load from short queries. The new
  behavior is disabled by default and can be enabled by setting the
  config property ``task.level-absolute-priority=true``.
* Improve the fairness of the local scheduler such that long-running queries
  which spend more time on the CPU per scheduling quanta (e.g., due to
  slow connectors) do not get a disproportionate share of CPU. The new
  behavior is disabled by default and can be enabled by setting the
  config property ``task.legacy-scheduling-behavior=false``.
* Add a config option to control the prioritization of queries based on
  elapsed scheduled time. The ``task.level-time-multiplier`` property
  controls the target scheduled time of a level relative to the next
  level. Higher values for this property increase the fraction of CPU
  that will be allocated to shorter queries. This config property only
  has an effect when ``task.level-absolute-priority=true`` and
  ``task.legacy-scheduling-behavior=false``.

Hive Changes
------------

* Fix potential native memory leak when writing tables using RCFile.
* Correctly categorize certain errors when writing tables using RCFile.
* Decrease the number of file system metadata calls when reading tables.
* Add support for dropping columns.

JDBC Driver Changes
-------------------

* Add support for query cancellation using ``Statement.cancel()``.

PostgreSQL Changes
------------------

* Add support for operations on external tables.

Accumulo Changes
----------------

* Improve query performance by scanning index ranges in parallel.

SPI Changes
-----------

* Fix regression that broke serialization for ``SchemaTableName``.
* Add access control check for ``DROP COLUMN``.
