============
CREATE TABLE
============

Synopsis
--------

.. code-block:: none

    CREATE TABLE table_name (
      column_name data_type [, ...]
    )

Description
-----------

Create a new, empty table with the specified columns.
Use :doc:`create-table-as` to create a table with data.

Examples
--------

Create a new table ``orders``::

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
