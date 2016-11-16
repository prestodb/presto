================
Table statistics
================

Presto supports statistics based optimizations for queries. For query to be subject for such optimization
statistics information must be available for tables used by query.

Table statistics are provided to query planner by connectors.
Currently the only connector supporting the mechanism is Hive connector.

Table layouts
-------------

Statistics are exposed to query planner on per table layout basis. Table layout represents a subset of table's data with attached
data organization properties (like sort order, bucketing etc.).

How many table layouts table has and details of table layout implementation are specific to connector.
Taking Hive connector as an example:

* not partitioned tables have just one table layout - representing all data in a table
* partitioned tables have a family of table layouts. Each subset of partitions to be scanned represent one table layout.
  Presto will try to pick up smallest possible layout based on filtering predicates from query.

Available statistics
--------------------

Currently following statics are available in Presto:
 * **rows count** - total number of rows for table layout
 * Per each column:

   * **data size** - summary column data size which needs to be read
   * **nulls count** - number of null values
   * **distinct values count** - estimated number of distinct values


Depending which connector is in use only, subset of statistics may be available.
For example Hive connector does not support data size for now.
Also some statistics may be available for one table but not the other,
or even one table layout and not the other for single table.

Displaying table statistics
---------------------------

Table statistics can be displayed via Presto CLI using ``SHOW STATS`` command.

There are two flavors of the command:
 * ``SHOW STATS FOR <table_name>`` - will show statistics for table layout representing all data in the table
 * ``SHOW STATS FOR (SELECT <column_list|*> FROM <table_name> WHERE <filtering_condition>)`` -
   will show statistics for table layout of table ``t`` representing subset of data after applying given filtering condition. Filtering condition used in
   ``WHERE`` and column list can reference table columns.

See the examples output for ``SHOW STATS`` command:

.. code-block:: none

    presto:default> SHOW STATS FOR nation;

     column_name | data_size | distinct_values_count | nulls_count | row_count
    -------------+-----------+-----------------------+-------------+-----------
     comment     | NULL      |                  31.0 |        31.0 | NULL
     nationkey   | NULL      |                  19.0 |        19.0 | NULL
     name        | NULL      |                  24.0 |        24.0 | NULL
     regionkey   | NULL      |                   5.0 |         5.0 | NULL
     NULL        | NULL      | NULL                  | NULL        |      25.0
     (5 rows)


    presto:default> SHOW STATS FOR (SELECT * FROM nation WHERE nationkey > 10);

     column_name | data_size | distinct_values_count | nulls_count | row_count
    -------------+-----------+-----------------------+-------------+-----------
     comment     | NULL      |                  21.0 |        21.0 | NULL
     nationkey   | NULL      |                   9.0 |         9.0 | NULL
     name        | NULL      |                  14.0 |        14.0 | NULL
     regionkey   | NULL      |                   3.0 |         3.0 | NULL
     NULL        | NULL      | NULL                  | NULL        |      15.0
     (5 rows)

There are two types of rows here.
For each column in table there is a row with ``column_name`` equal to name of column.
Such rows expose column-related statistics for a table (data size, nulls count, distinct values count).

Additionally there is one row with NULL as ``column_name``. This row contains table-layout wide statistics - for now just row count.


Updating Hive tables statistics
-------------------------------

Presto makes use of statistics which are managed by Hive and exposed via Hive metastore API.
Depending on Hive configuration table statistics may be or may be not updated automatically.

To trigger updating table statistics user have to use Hive CLI.

Following command should be used to update table statistics for non-partitioned table ``t``::

        ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS

For partitioned tables partitions specification must be used.
Assuming table ``t`` has two partitioning keys ``a`` and ``b``, following command should be used
to update table statistics for all partitions::

        ANALYZE TABLE t PARTITION (a, b) COMPUTE STATISTICS FOR COLUMNS``

It is also possible to update statistics for just subset of partitions.
This command will update statistics for all partitions for which partitioning key ``a`` is equal to ``1``::

        ANALYZE TABLE t PARTITION (a=1, b) COMPUTE STATISTICS FOR COLUMNS``

And this command will update stats for just one partition::

        ANALYZE TABLE t PARTITION (a=1, b=5) COMPUTE STATISTICS FOR COLUMNS``

For documentation of Hive statistics mechanism see https://cwiki.apache.org/confluence/display/Hive/StatsDev
