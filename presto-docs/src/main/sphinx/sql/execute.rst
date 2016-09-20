=======
EXECUTE
=======

Synopsis
--------

.. code-block:: none

    EXECUTE statement_name [ USING parameter1 [ , parameter2, ... ] ]

Description
-----------

Executes a prepared statement with the name ``statement_name``. Parameter values
are defined in the ``USING`` clause.

Examples
--------

Prepare and execute a query with no parameters::

    PREPARE my_select1 FROM
    SELECT name FROM nation;

.. code-block:: sql

    EXECUTE my_select1;

Prepare and execute a query with two parameters::

    PREPARE my_select2 FROM
    SELECT name FROM nation WHERE regionkey = ? and nationkey < ?;

.. code-block:: sql

    EXECUTE my_select2 USING 1, 3;

This is equivalent to::

   SELECT name FROM nation WHERE regionkey = 1 AND nationkey < 3;

See Also
--------

:doc:`prepare`
