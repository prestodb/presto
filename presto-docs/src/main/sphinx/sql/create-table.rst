============
CREATE TABLE
============

Synopsis
--------

.. code-block:: none

    CREATE TABLE [ IF NOT EXISTS ]
    table_name (
      column_name data_type [, ...]
    )
    [ WITH ( property_name = expression [, ...] ) ]


Description
-----------

Create a new, empty table with the specified columns.
Use :doc:`create-table-as` to create a table with data.

The optional ``IF NOT EXISTS`` clause causes the error to be
suppressed if the table already exists.

The optional ``WITH`` clause can be used to set properties
on the newly created table.  To list all available table
properties, run the following query::

    SELECT * FROM system.metadata.table_properties

Examples
--------

Create a new table ``orders``::

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
    WITH (format = 'ORC')

Create the table ``orders`` if it does not already exist::

    CREATE TABLE IF NOT EXISTS orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )

See Also
--------

:doc:`alter-table`, :doc:`drop-table`, :doc:`create-table-as`, :doc:`show-create-table`
