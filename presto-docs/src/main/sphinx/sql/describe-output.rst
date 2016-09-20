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
catalog, schema, table, type, type size in bytes, and a boolean indicating
whether the column is aliased.

Examples
--------

Prepare and describe a query with four output columns:

.. code-block:: sql

    PREPARE my_select1 FROM
    SELECT * FROM nation

.. code-block:: sql

   DESCRIBE OUTPUT my_select1;

.. code-block:: none

         Column Name | Catalog | Schema | Table  |  Type   | Type Size | Aliased
        -------------+---------+--------+--------+---------+-----------+---------
         nationkey   | tpch    | sf1    | nation | bigint  |         8 | false
         name        | tpch    | sf1    | nation | varchar |         0 | false
         regionkey   | tpch    | sf1    | nation | bigint  |         8 | false
         comment     | tpch    | sf1    | nation | varchar |         0 | false
        (4 rows)

Prepare and describe a query whose output columns are expressions:

.. code-block:: sql

   PREPARE my_select2 FROM
   SELECT count(*) as my_count, 1+2 FROM nation

.. code-block:: sql

    DESCRIBE OUTPUT my_select2;

.. code-block:: none

         Column Name | Catalog | Schema | Table |  Type  | Type Size | Aliased
        -------------+---------+--------+-------+--------+-----------+---------
         my_count    |         |        |       | bigint |         8 | true
         _col1       |         |        |       | bigint |         8 | false
        (2 rows)

Prepare and describe a row count query:

.. code-block:: sql

    PREPARE my_create FROM
    CREATE TABLE foo AS SELECT * FROM nation

.. code-block:: sql

    DESCRIBE OUTPUT my_create;

.. code-block:: none

     Column Name | Catalog | Schema | Table |  Type  | Type Size | Aliased
    -------------+---------+--------+-------+--------+-----------+---------
     rows        |         |        |       | bigint |         8 | false
    (1 row)

See Also
--------

:doc:`prepare`
