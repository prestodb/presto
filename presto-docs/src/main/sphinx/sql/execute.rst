.. _execute:

=======
EXECUTE
=======

Synopsis
--------

.. code-block:: none

    EXECUTE statement_name [USING parameter1[, parameter2, ...]]

Description
-----------

Executes a prepared statement with the name ``statement_name``. Parameter values
are defined in the USING clause.

Examples
--------

* Execute a query with no parameters:

  Let ``my_select1`` be the name of the prepared statement ``SELECT name FROM nation``.

  The command below will execute the query::

    EXECUTE my_select1;

* Execute a query with a single parameter:

  Let ``my_select2`` be the name of the prepared statement ``SELECT name FROM nation WHERE regionkey = ?``.

  The command below will execute ``SELECT name FROM nation WHERE regionkey = 1``::

    EXECUTE my_select2 USING 1;

* Execute a query with two parameters:

  Let ``my_select3`` be the name of the prepared statement
  ``SELECT name FROM nation WHERE regionkey = ? and nationkey < ?``.

  The command below will execute ``SELECT name from nation WHERE regionkey = 1 AND nationkey < 3``::

    EXECUTE my_select3 USING 1, 3;

