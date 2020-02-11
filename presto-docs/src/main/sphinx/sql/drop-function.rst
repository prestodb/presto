=============
DROP FUNCTION
=============

Synopsis
--------

.. code-block:: none

    DROP FUNCTION [ IF EXISTS ] qualified_function_name [ ( parameter_type[, ...] ) ]


Description
-----------

Drop an existing function.

The optional ``IF EXISTS`` clause causes the ``NOT_FOUND`` error
to be suppressed if the function does not exists.

Each ``DROP FUNCTION`` statement can only drop one function
at a time. If multiple functions are matched by not specifying
the parameter type list, the query would fail.


Examples
--------

Drop the function ``example.default.tan(double)``::

    DROP FUNCTION example.default.tan(double)

If only one function exists for ``example.default.tan``, parameter type list may be omitted::

    DROP FUNCTION example.default.tan

Drop the function ``example.default.tan(double)`` if it exists::

    DROP FUNCTION IF EXISTS example.default.tan(double)


See Also
--------

:doc:`create-function`, :doc:`alter-function`, :doc:`show-functions`
