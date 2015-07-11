===============
CREATE TABLE AS
===============

Synopsis
--------

.. code-block:: none

    CREATE TABLE [ IF NOT EXISTS ] table_name AS query

Description
-----------

Create a new table containing the result of a :doc:`select` query.
Use :doc:`create-table` to create an empty table.

The optional ``IF NOT EXISTS`` clause causes the error to be
suppressed if the table already exists.

Examples
--------

Create a new table ``orders_by_date`` that summarizes ``orders``::

    CREATE TABLE orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

Create the table ``orders_by_date`` if it does not already exist::

    CREATE TABLE IF NOT EXISTS orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate
