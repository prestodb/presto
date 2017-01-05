============
CREATE TABLE
============

Synopsis
--------

.. code-block:: none

    CREATE TABLE [ IF NOT EXISTS ]
    table_name (
      { column_name data_type [ COMMENT comment ]
      | LIKE existing_table_name [ { INCLUDING | EXCLUDING } PROPERTIES ] }
      [, ...]
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

The ``LIKE`` clause can be used to include all the column definitions from
an existing table in the new table. Multiple ``LIKE`` clauses may be
specified, which allows copying the columns from multiple tables.

If ``INCLUDING PROPERTIES`` is specified, all of the table properties are
copied to the new table. If the ``WITH`` clause specifies the same property
name as one of the copied properties, the value from the ``WITH`` clause
will be used. The default behavior is ``EXCLUDING PROPERTIES``. The
``INCLUDING PROPERTIES`` option maybe specified for at most one table.

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

Create the table ``bigger_orders`` using the columns from ``orders``
plus additional columns at the start and end::

    CREATE TABLE bigger_orders (
      another_orderkey bigint,
      LIKE orders,
      another_orderdate date
    )

See Also
--------

:doc:`alter-table`, :doc:`drop-table`, :doc:`create-table-as`, :doc:`show-create-table`
