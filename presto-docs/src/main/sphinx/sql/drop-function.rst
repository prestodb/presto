=============
DROP FUNCTION
=============

Synopsis
--------

.. code-block:: none

    DROP [TEMPORARY] FUNCTION [ IF EXISTS ] qualified_function_name [ ( parameter_type[, ...] ) ]


Description
-----------

Drop existing functions that matches the given function name.

The optional parameter type list can be specified to narrow down
the match to a specific function signature.

The optional ``IF EXISTS`` clause causes the ``NOT_FOUND`` error
to be suppressed if no matching function signature exists.

When ``TEMPORARY`` is specified, temporary functions with matching
criteria are dropped.


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
