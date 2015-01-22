.. _array_functions:

=============================
Array Functions and Operators
=============================

Subscript Operator: []
----------------------

The ``[]`` operator is used to access an element of an array, and is indexed starting from one::

    SELECT my_array[1] AS first_element

Concatenation Operator: ||
--------------------------
The ``||`` operator can be used to concatenate an array with an array or an element of the same type::

    SELECT ARRAY [1] || ARRAY [2]; => [1, 2]
    SELECT ARRAY [1] || 2; => [1, 2]
    SELECT 2 || ARRAY [1]; => [2, 1]

Array Functions
---------------

.. function:: cardinality(x) -> bigint

    Returns the cardinality (size) of the array ``x``.

.. function:: contains(x, y) -> boolean

    Returns true iff the array ``x`` contains the element ``y``.

.. function:: array_sort(x) -> array

    Sorts and returns the array ``x``. The elements of ``x`` must be orderable.

.. function:: concat(x, y) -> array
    :noindex:

    Concatenates given arrays ``x`` and ``y``. The element types of ``x`` and ``y`` must be the same.
    This function provides the same functionality as the concatenation operator (``||``).
