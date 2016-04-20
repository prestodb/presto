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

.. function:: array_join(x, delimiter, null_replacement) -> varchar

    Concatenates the elements of the given array using the delimiter and an optional string to replace nulls.

.. function:: array_max(x) -> x

    Returns the maximum value of input array.

.. function:: array_min(x) -> x

    Returns the minimum value of input array.

.. function:: array_position(x, element) -> bigint

    Returns the position of the first occurrence of the ``element`` in array ``x`` (or 0 if not found).

.. function:: array_remove(x, element) -> array

    Remove all elements that equal ``element`` from array ``x``.

.. function:: array_sort(x) -> array

    Sorts and returns the array ``x``. The elements of ``x`` must be orderable.

.. function:: cardinality(x) -> bigint

    Returns the cardinality (size) of the array ``x``.

.. function:: concat(x, y) -> array
    :noindex:

    Concatenates the arrays ``x`` and ``y``. This function provides the same
    functionality as the SQL-standard concatenation operator (``||``).

.. function:: contains(x, element) -> boolean

    Returns true if the array ``x`` contains the ``element``.

.. function:: element_at(array<E>, index) -> E

    Returns element of ``array`` at given ``index``.
    If ``index`` >= 0, this function provides the same functionality as the SQL-standard subscript operator (``[]``).
    If ``index`` < 0, ``element_at`` accesses elements from the last to the first.

.. function:: slice(x, start, length) -> array

    Subsets array ``x`` starting from index ``start`` (or starting from the end if ``start`` is negative) with a length of ``length``.
