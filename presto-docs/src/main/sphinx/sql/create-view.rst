===========
CREATE VIEW
===========

Synopsis
--------

.. code-block:: none

    CREATE [ OR REPLACE ] VIEW view_name
    [COMMENT view_comment]
    AS query

Description
-----------

Create a new view of a :doc:`select` query. The view is a logical table
that can be referenced by future queries. Views do not contain any data.
Instead, the query stored by the view is executed everytime the view is
referenced by another query.

The optional ``OR REPLACE`` clause causes the view to be replaced if it
already exists rather than raising an error.

Examples
--------

Create a simple view ``test`` over the ``orders`` table::

    CREATE VIEW test AS
    SELECT orderkey, orderstatus, totalprice / 2 AS half
    FROM orders

Create a view ``orders_by_date`` that summarizes ``orders`` with a comment::

    CREATE VIEW orders_by_date
    COMMENT 'a view to track orders by date'
    AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

Create a view that replaces an existing view::

    CREATE OR REPLACE VIEW test AS
    SELECT orderkey, orderstatus, totalprice / 4 AS quarter
    FROM orders

See Also
--------

:doc:`drop-view`, :doc:`show-create-view`
