.. _array_functions:

=============================
Array Functions and Operators
=============================

Subscript Operator: []
----------------------

The ``[]`` operator is used to access an element of an array, and is indexed starting from one::

    SELECT my_array[1] AS first_element

Array Functions
---------------

.. function:: cardinality(x) -> bigint

    Returns the cardinality (size) of the array ``x``.

.. function:: contains(x, y) -> boolean

    Returns true iff the array ``x`` contains the element ``y``.

.. function:: array_sort(x) -> array

    Sorts and returns the array ``x``. The elements of ``x`` must be orderable.
