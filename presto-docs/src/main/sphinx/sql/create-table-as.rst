===============
CREATE TABLE AS
===============

Synopsis
--------

.. code-block:: none

    CREATE TABLE table_name
    [ WITH ( property_name = expression [, ...] ) ]
    AS query

Description
-----------

Create a new table containing the result of a :doc:`select` query.
Use :doc:`create-table` to create an empty table.

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
