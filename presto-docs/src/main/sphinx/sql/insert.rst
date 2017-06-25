======
INSERT
======

Synopsis
--------

.. code-block:: none

    INSERT INTO table_name [ ( column [, ... ] ) ] query

Description
-----------

Insert new rows into a table.

If the list of column names is specified, they must exactly match the list
of columns produced by the query. Each column in the table not present in the
column list will be filled with a ``null`` value. Otherwise, if the list of
columns is not specified, the columns produced by the query must exactly match
the columns in the table being inserted into.


Examples
--------

Load additional rows into the ``orders`` table from the ``new_orders`` table::

    INSERT INTO orders
    SELECT * FROM new_orders;

Insert a single row into the ``cities`` table::

    INSERT INTO cities VALUES (1, 'San Francisco');

Insert multiple rows into the ``cities`` table::

    INSERT INTO cities VALUES (2, 'San Jose'), (3, 'Oakland');

Insert a single row into the ``nation`` table with the specified column list::

    INSERT INTO nation (nationkey, name, regionkey, comment)
    VALUES (26, 'POLAND', 3, 'no comment');

Insert a row without specifying the ``comment`` column.
That column will be ``null``::

    INSERT INTO nation (nationkey, name, regionkey)
    VALUES (26, 'POLAND', 3);

See Also
--------

:doc:`values`
