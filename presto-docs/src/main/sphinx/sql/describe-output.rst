===============
DESCRIBE OUTPUT
===============

Synopsis
--------

.. code-block:: none

    DESCRIBE OUTPUT statement_name

Description
-----------

Describes the output columns of a prepared statement.  It returns a table
with metadata about each output column. Each row in the output provides the
following information: column name (or the column's alias if it is aliased),
table, schema, connector, type, type size in bytes, boolean indicating whether
the column is aliased and boolean indicating whether the query is a row count
query (DDL/DML query that returns no data).

Examples
--------

1. Describe a query with four output columns:

   Let ``my_select`` be the name of the prepared statement ``SELECT * FROM nation``::

       DESCRIBE OUTPUT my_select;

   Returns a table with metadata on all output columns ::

         Column Name | Table  | Schema | Connector |  Type   | Type Size | Aliased | Row Count Query
        -------------+--------+--------+-----------+---------+-----------+---------+-----------------
         nationkey   | nation | sf1    | tpch      | bigint  |         8 | false   | false
         name        | nation | sf1    | tpch      | varchar |         0 | false   | false
         regionkey   | nation | sf1    | tpch      | bigint  |         8 | false   | false
         comment     | nation | sf1    | tpch      | varchar |         0 | false   | false

2. Describe a query whose output columns are expressions:

   Let ``my_select`` be the name of the prepared statement ``SELECT count(*) as my_count, 1+2 FROM nation``::

       DESCRIBE OUTPUT my_select;

   Returns a table with two rows ::

         Column Name | Table | Schema | Connector |  Type  | Type Size | Aliased | Row Count Query
        -------------+-------+--------+-----------+--------+-----------+---------+-----------------
         my_count    |       |        |           | bigint |         8 | true    | false
         _col1       |       |        |           | bigint |         8 | false   | false


3. Describe a row count query:

    Let ``my_show_tables`` be the name of the prepared statement ``show tables``::

        DESCRIBE OUTPUT my_show_tables;

    Returns a table with one null row ::

         Column Name | Table | Schema | Connector | Type | Type Size | Aliased | Row Count Query
        -------------+-------+--------+-----------+------+-----------+---------+-----------------
         NULL        | NULL  | NULL   | NULL      | NULL | NULL      | NULL    | true

