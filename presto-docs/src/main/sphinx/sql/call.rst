====
CALL
====

Synopsis
--------

.. code-block:: none

    CALL procedure_name ( [ name => ] expression [, ...] )

Description
-----------

Call a procedure.

Procedures can be provided by connectors to perform data manipulation or
administrative tasks. For example, the :doc:`/connector/system` defines a
procedure for killing a running query.

Some connectors, such as the :doc:`/connector/postgresql`, are for systems
that have their own stored procedures. These stored procedures are separate
from the connector-defined procedures discussed here and thus are not
directly callable via ``CALL``.

See connector documentation for details on available procedures.

Examples
--------

Call a procedure using positional arguments::

    CALL test(123, 'apple');

Call a procedure using named arguments::

    CALL test(name => 'apple', id => 123);

Call a procedure using a fully qualified name::

    CALL catalog.schema.test();
