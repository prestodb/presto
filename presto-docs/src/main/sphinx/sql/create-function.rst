===============
CREATE FUNCTION
===============

Synopsis
--------

.. code-block:: none

    CREATE [ OR REPLACE ] [TEMPORARY] FUNCTION
    qualified_function_name (
      parameter_name parameter_type
      [, ...]
    )
    RETURNS return_type
    [ COMMENT function_description ]
    [ LANGUAGE [ SQL | identifier] ]
    [ DETERMINISTIC | NOT DETERMINISTIC ]
    [ RETURNS NULL ON NULL INPUT | CALLED ON NULL INPUT ]
    [ RETURN expression | EXTERNAL [ NAME identifier ] ]


Description
-----------

Create a new function with the specified definition.

When ``TEMPORARY`` is specified, the created function is valid and visible
within the current session, but no persistent entry is made.

Each permanent function is uniquely identified by its qualified function name
and its parameter type list. ``qualified_function_name`` needs to be in
the format of ``catalog.schema.function_name``.

Each temporary functions is uniquely identified by the function name.
The name cannot be qualified, or collide with the name of an existing built-in
function.

In order to create a permanent function, the corresponding function namespace
(in the format ``catalog.schema``) must first be managed by a function
namespace manager (See :doc:`/admin/function-namespace-managers`).

The optional ``OR REPLACE`` clause causes the query to quietly replace
the existing function if a function with the identical signature (function
name with parameter type list) exists.

The ``return_type`` needs to match the actual type of the routine body
``expression``, without performing type coercion.

A set of routine characteristics can be specified to decorate the
function and specify its behavior. Each kind of routine characteristic
can be specified at most once.

============================ ======================== ================================================================
Routine Characteristic       Default Value            Description
============================ ======================== ================================================================
Language clause              SQL                      The language in which the function is defined.
Deterministic characteristic NOT DETERMINISTIC        Whether the function is deterministic. ``NOT DETERMINISTIC``
                                                      means that the function is possibly non-deterministic.
Null-call clause             CALLED ON NULL INPUT     The behavior of the function in which ``null`` is supplied as
                                                      the value of at least one argument.
============================ ======================== ================================================================


Examples
--------

Create a new function ``example.default.tan(double)``::

    CREATE FUNCTION example.default.tan(x double)
    RETURNS double
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    RETURN sin(x) / cos(x)

Create the table ``example.default.tan(double)`` if it does not already
exist, adding a function description and explicitly listing all the supported
routine characteristics::

    CREATE OR REPLACE FUNCTION example.default.tan(x double)
    RETURNS double
    COMMENT 'tangent trigonometric function'
    LANGUAGE SQL
    DETERMINISTIC
    RETURNS NULL ON NULL INPUT
    RETURN sin(x) / cos(x)

Create a new temporary function ``square``::

    CREATE TEMPORARY FUNCTION square(x int)
    RETURNS int
    RETURN x * x

See Also
--------

:doc:`alter-function`, :doc:`drop-function`, :doc:`show-functions`
