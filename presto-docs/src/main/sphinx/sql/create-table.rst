============
CREATE TABLE
============

Synopsis
--------

.. code-block:: none

    CREATE TABLE table_name AS query

Description
-----------

Create a new table containing the result of a :doc:`select` query.

Examples
--------

Create a new table ``orders_by_date`` that summarizes ``orders``::

    CREATE TABLE orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate
