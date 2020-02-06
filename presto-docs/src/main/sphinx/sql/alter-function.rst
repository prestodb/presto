==============
ALTER FUNCTION
==============

Synopsis
--------

.. code-block:: none

    ALTER FUNCTION qualified_function_name [ ( parameter_type[, ...] ) ]
    RETURNS NULL ON NULL INPUT | CALLED ON NULL INPUT


Description
-----------

Alter the definition of an existing function.

Parameter type list must be specified if multiple signatures exist
for the specified function name. If exactly one signature exists for
the specified function name, parameter type list can be omitted.

Currently, only modifying the null-call clause is supported.


Examples
--------

Change the null-call clause of a function ``example.default.tan(double)``::

    ALTER FUNCTION prod.default.tan(double)
    CALLED ON NULL INPUT

If only one function exists for ``example.default.tan``, parameter type list may be omitted::

    ALTER FUNCTION prod.default.tan
    CALLED ON NULL INPUT


See Also
--------

:doc:`create-function`, :doc:`drop-function`, :doc:`show-functions`
