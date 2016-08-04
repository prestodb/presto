===============
CREATE TABLE AS
===============

Synopsis
--------

.. code-block:: none

    CREATE TABLE [ IF NOT EXISTS ] table_name
    [ WITH ( property_name = expression [, ...] ) ]
    AS query
    [ WITH [ NO ] DATA ]

Description
-----------

Create a new table containing the result of a :doc:`select` query.
Use :doc:`create-table` to create an empty table.

The optional ``IF NOT EXISTS`` clause causes the error to be
suppressed if the table already exists.

The optional ``WITH`` clause can be used to set properties
on the newly created table.  To list all available table
properties, run the following query::

    SELECT * FROM system.metadata.table_properties

Examples
--------

Create a new table ``orders_by_date`` that summarizes ``orders``::

    CREATE TABLE orders_by_date
    WITH (format = 'ORC')
    AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

Create the table ``orders_by_date`` if it does not already exist::

    CREATE TABLE IF NOT EXISTS orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

Create a new ``empty_nation`` table with the same schema as ``nation`` and no data::

    CREATE TABLE empty_nation AS
    SELECT *
    FROM nation
    WITH NO DATA

See Also
--------

:doc:`create-table`, :doc:`select`
