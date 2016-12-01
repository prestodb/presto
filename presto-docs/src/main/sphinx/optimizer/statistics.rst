================
Table Statistics
================

Presto supports statistics based optimizations for queries. For a query to take advantage of these optimizations,
Presto must have statistical information for the tables in that query.

Table statistics are provided to the query planner by connectors.
Currently the only connector that supports statistics is the Hive connector.

Table Layouts
-------------

Statistics are exposed to the query planner by table layout. A table layout represents a subset of a table's data
and contains information about the organizational properties of that data (like sort order and bucketing).

The number of table layouts available for a table and the details of those table layouts are specific to each connector.
Using the Hive connector as an example:

* Non-partitioned tables have just one table layout representing all data in the table
* Partitioned tables have a family of table layouts. Each set of partitions to be scanned represents one table layout.
  Presto will try to pick a table layout consisting of the smallest number of partitions based on filtering predicates
  from the query.

Available Statistics
--------------------

Currently, the following statistics are available in Presto:

 * For the whole table:

   * **row count**: the total number of rows for the table layout

 * For each column:

   * **data size**: the data size that needs to be read
   * **null count**: the number of null values
   * **distinct value count**: the estimated number of distinct values


The set of statistics available for a particular query depends on the connector being used and can also vary by table or
even table layout. For example, the Hive connector does not currently provide statistics on data size.

Displaying Table Statistics
---------------------------

Table statistics can be displayed via the Presto CLI using the ``SHOW STATS`` command.
There are two flavors of the command:

 * ``SHOW STATS FOR <table_name>`` will show statistics for the table layout representing all data in the table
 * ``SHOW STATS FOR (SELECT <column_list|*> FROM <table_name> WHERE <filtering_condition>)``
   will show statistics for the table layout of table ``t`` representing a subset of data after applying the given filtering
   condition. Both the column list and the filtering condition used in the ``WHERE`` clause can reference table columns.

In both cases, the ``SHOW STATS`` command outputs two types of rows.
For each column in the table there is a row with ``column_name`` equal to the name of that column.
These rows expose column-related statistics for a table (data size, nulls count, distinct values count).
Additionally there is one row with NULL as the ``column_name``. This row contains table-layout wide statistics - for now just row count.

For example:

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

If provided ``SELECT`` will filter out all of the partitions (all table layouts),
than the ``SHOW STATS`` will return no statistic which will be represented as in example below.

.. code-block:: none

    presto:default> SHOW STATS FOR (SELECT * FROM nation WHERE nationkey > 999);

     column_name
    -------------
     NULL
    (1 row)

Note, that currently providing ``column_list`` instead of ``*`` in ``SELECT`` will not influence the output table.

For example:

.. code-block:: none

    presto:default> SHOW STATS FOR (SELECT comment FROM nation WHERE nationkey > 10);

     column_name | data_size | distinct_values_count | nulls_count | row_count
    -------------+-----------+-----------------------+-------------+-----------
     comment     | NULL      |                  21.0 |        21.0 | NULL
     nationkey   | NULL      |                   9.0 |         9.0 | NULL
     name        | NULL      |                  14.0 |        14.0 | NULL
     regionkey   | NULL      |                   3.0 |         3.0 | NULL
     NULL        | NULL      | NULL                  | NULL        |      15.0
     (5 rows)


Updating Statistics for Hive Tables
-----------------------------------

For the Hive connector, Presto uses the statistics that are managed by Hive and exposed via the Hive metastore API.
Depending on the Hive configuration, table statistics may not be updated automatically.

If statistics are not updated automatically, the user needs to trigger a statistics update via the Hive CLI.

The following command can be used in the Hive CLI to update table statistics for non-partitioned table ``t``::

        ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS

For partitioned tables, partitioning information must be specified in the command.
Assuming table ``t`` has two partitioning keys ``a`` and ``b``, the following command would
update the table statistics for all partitions::

        ANALYZE TABLE t PARTITION (a, b) COMPUTE STATISTICS FOR COLUMNS``

It is also possible to update statistics for just a subset of partitions.
This command will update statistics for all partitions for which partitioning key ``a`` is equal to ``1``::

        ANALYZE TABLE t PARTITION (a=1, b) COMPUTE STATISTICS FOR COLUMNS``

And this command will update statistics for just one partition::

        ANALYZE TABLE t PARTITION (a=1, b=5) COMPUTE STATISTICS FOR COLUMNS``

For documentation on Hive's statistics mechanism see https://cwiki.apache.org/confluence/display/Hive/StatsDev
