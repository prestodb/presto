===============
CREATE TABLE AS
===============

Synopsis
--------

.. code-block:: none

    CREATE TABLE table_name AS query

Description
-----------

Create a new table containing the result of a :doc:`select` query.
Use :doc:`create-table` to create an empty table.

Examples
--------

Create a new table ``orders_by_date`` that summarizes ``orders``::

    CREATE TABLE orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

WITH NO DATA Clause
--------

The WITH NO DATA clause specifies that the data rows that result
from evaluating the query expression are not used; only the names
and data types of the columns in the query result are used.
The WITH NO DATA clause must be specified.

.. code-block:: none

    CREATE TABLE table_name AS query WITH NO DATA