======
UPDATE
======

Synopsis
--------

.. code-block:: none

    UPDATE table_name SET [ column = expression [, ... ] ] [ WHERE condition ]

Description
-----------

Update selected columns values in existing rows in a table. 

The columns named in the ``column = expression`` assignments will be updated
for all rows that match the ``WHERE`` condition.  The values of all column update
expressions for a matching row are evaluated before any column value is changed.
When the type of the expression and the type of the column differ, the usual implicit
CASTs, such as widening numeric fields, are applied to the ``UPDATE`` expression values.


Examples
--------

Update the status of all purchases that haven't been assigned a ship date::

    UPDATE purchases SET status = 'OVERDUE' WHERE ship_date IS NULL;

Update the account manager and account assign date for all customers::

    UPDATE customers SET
      account_manager = 'John Henry',
      assign_date = DATE '2007-01-01';
