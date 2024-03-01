==========
DROP TABLE
==========

Synopsis
--------

.. code-block:: none

    DROP TABLE  [ IF EXISTS ] table_name

Description
-----------

Drops an existing table.

The optional ``IF EXISTS`` clause causes the error to be suppressed if
the table does not exist.

Examples
--------

Drop the table ``orders_by_date``::

    DROP TABLE orders_by_date

Drop the table ``orders_by_date`` if it exists::

    DROP TABLE IF EXISTS orders_by_date

See Also
--------

:doc:`alter-table`, :doc:`create-table`
