======
INSERT
======

Synopsis
--------

.. code-block:: none

    INSERT INTO table_name query

Description
-----------

Insert new rows into a table.

.. note::

    Currently, the list of column names cannot be specified. Thus,
    the columns produced by the query must exactly match the columns
    in the table being inserted into.

Examples
--------

Load additional rows into the ``orders`` table from the ``new_orders`` table::

    INSERT INTO orders
    SELECT * FROM new_orders;

Insert a single row into the ``cities`` table::

    INSERT INTO cities VALUES (1, 'San Francisco');

Insert multiple rows into the ``cities`` table::

    INSERT INTO cities VALUES (2, 'San Jose'), (3, 'Oakland');
