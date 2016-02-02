=======
PREPARE
=======

Synopsis
--------

.. code-block:: none

    PREPARE statement_name FROM statement

Description
-----------

Prepares a statement for execution later. Prepared statements are queries that
are saved on a per-session basis with a given name. The statement can include
parameters in place of literals to be filled in at execution time. Parameters
are represented by question marks.


Examples
--------

Prepare a select query::

    PREPARE my_select1
    FROM SELECT * from NATION;

Prepare a select query that includes parameters. The values to compare with
regionkey and nationkey will be filled in with the :ref:`execute` statement::

    PREPARE my_select2
    FROM SELECT name FROM nation WHERE regionkey = ? and nationkey < ?;

Prepare an insert::

    PREPARE my_insert
    FROM INSERT INTO cities VALUES (1, 'San Francisco');

