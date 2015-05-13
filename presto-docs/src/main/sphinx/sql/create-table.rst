============
CREATE TABLE
============

Synopsis
--------

.. code-block:: none

    CREATE TABLE [ IF NOT EXISTS ] table_name (
      column_name data_type [, ...]
    )

Description
-----------

Create a new, empty table with the specified columns.
Use :doc:`create-table-as` to create a table with data.

The optional ``IF NOT EXISTS`` clause causes the error to be
suppressed if the table already exists.

Examples
--------

Create a new table ``orders``::

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )

Create the table ``orders`` if it does not already exist::

    CREATE TABLE IF NOT EXISTS orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
