====
CALL
====

Synopsis
--------

.. code-block:: none

    CALL procedure_name ( [ name => ] expression [, ...] )

Description
-----------

Call a procedure defined for a given connector.

Examples
--------

Call a procedure using positional arguments::

    CALL test(123, 'apple');

Call a procedure using named arguments::

    CALL test(name => 'apple', id => 123);

Call a procedure using a fully qualified name::

    CALL catalog.schema.test();

.. note::
    CALL statement is NOT used to invoke a DBMS style stored procedure from the underlying
    system like PostgreSQL or MySQL.
