==============
DESCRIBE INPUT
==============

Synopsis
--------

.. code-block:: none

    DESCRIBE INPUT statement_name

Description
-----------

Describes the input parameters for a prepared statement.  It returns a table
with the position and type of each parameter.  If the type of the parameter can
not be determined, unknown is returned.


Examples
--------

1. Describe query with 3 parameters:

   Let ``my_select`` be the name of the prepared statement ``SELECT ? FROM nation WHERE regionkey = ? AND name < ?``::

       DESCRIBE INPUT my_select

   Returns a table with the positions and types of all parameters ::

       Position | Type
       -----------------
       0        | unknown
       1        | bigint
       2        | varchar

2. Describe query with no parameters:

   Let ``my_select`` be the name of the prepared statement ``SELECT * FROM nation``::

       DESCRIBE INPUT my_select2

   Returns a table with one null row ::

       Position | Type
       ---------------
       NULL     | NULL

