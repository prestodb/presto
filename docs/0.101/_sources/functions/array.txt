=============================
Array Functions and Operators
=============================

Subscript Operator: []
----------------------

The ``[]`` operator is used to access an element of an array and is indexed starting from one::

    SELECT my_array[1] AS first_element

Concatenation Operator: ||
--------------------------

The ``||`` operator is used to concatenate an array with an array or an element of the same type::

    SELECT ARRAY [1] || ARRAY [2]; => [1, 2]
    SELECT ARRAY [1] || 2; => [1, 2]
    SELECT 2 || ARRAY [1]; => [2, 1]

Array Functions
---------------

.. function:: array_distinct(x) -> array

    Remove duplicate values from the array ``x``.

.. function:: array_intersect(x, y) -> array

    Returns an array of the elements in the intersection of ``x`` and ``y``, without duplicates.

.. function:: array_position(array, element) -> bigint

    Returns the position of the first occurrence of the ``element`` in ``array`` (or 0 if not found).

.. function:: array_sort(x) -> array

    Sorts and returns the array ``x``. The elements of ``x`` must be orderable.

.. function:: cardinality(x) -> bigint

    Returns the cardinality (size) of the array ``x``.

.. function:: concat(x, y) -> array
    :noindex:

    Concatenates the arrays ``x`` and ``y``. This function provides the same
    functionality as the SQL-standard concatenation operator (``||``).

.. function:: contains(array, element) -> boolean

    Returns true if the ``array`` contains the ``element``.
