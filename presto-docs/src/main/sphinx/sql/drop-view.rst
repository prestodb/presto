=========
DROP VIEW
=========

Synopsis
--------

.. code-block:: none

    DROP VIEW [ IF EXISTS ] view_name

Description
-----------

Drop an existing view.

The optional ``IF EXISTS`` clause causes the error to be suppressed if
the view does not exist.

Examples
--------

Drop the view ``orders_by_date``::

    DROP VIEW orders_by_date

Drop the view ``orders_by_date`` if it exists::

    DROP VIEW IF EXISTS orders_by_date

See Also
--------

:doc:`create-view`
