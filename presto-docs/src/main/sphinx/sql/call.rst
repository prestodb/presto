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

CALL statement supports passing arguments by name or by position. Mixing position and named arguments is not supported.

* Named arguments

Procedure arguments all have a name. When passing arguments by name, arguments can be in any order
and any optional argument can be omitted::

    CALL catalog_name.system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1);

* Positional arguments

When passing arguments by position, only the ending arguments may be omitted if they are optional.
Intermediate parameters cannot be omitted::

    CALL catalog_name.system.procedure_name(arg_1, arg_2, ... arg_n);

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
