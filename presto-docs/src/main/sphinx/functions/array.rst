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

    SELECT ARRAY [1] || ARRAY [2]; -- [1, 2]
    SELECT ARRAY [1] || 2; -- [1, 2]
    SELECT 2 || ARRAY [1]; -- [2, 1]

Array Functions
---------------

.. function:: array_distinct(x) -> array

    Remove duplicate values from the array ``x``.

        SELECT array_distinct(ARRAY[1,1,2,2,3,3]); -- [1, 2, 3]
        SELECT array_distinct(ARRAY['a','a','b','b','c','c']); -- ['a', 'b', 'c']

.. function:: array_intersect(x, y) -> array

    Returns an array of the elements in the intersection of ``x`` and ``y``, without duplicates.

        SELECT array_intersect(ARRAY[1,2,3], ARRAY[2,3,4]); -- [2, 3]
        SELECT array_intersect(ARRAY['a','a','b'], ARRAY['b', 'c']); -- ['b']

.. function:: array_union(x, y) -> array

    Returns an array of the elements in the union of ``x`` and ``y``, without duplicates.

        SELECT array_union(ARRAY[1,1,2], ARRAY[2,3]); -- [1, 2, 3]
        SELECT array_union(ARRAY['a','a','b'], ARRAY['b', 'c']); -- ['a', 'b', 'c']

.. function:: array_except(x, y) -> array

    Returns an array of elements in ``x`` but not in ``y``, without duplicates.

.. function:: array_join(x, delimiter, null_replacement) -> varchar

    Concatenates the elements of the given array using the delimiter and an optional string to replace nulls.

        SELECT array_join(ARRAY[1,1,2], '|'); -- 1|1|2
        SELECT array_join(ARRAY[1,1,null], '|', 'NaN'); -- 1|1|NaN

.. function:: array_max(x) -> x

    Returns the maximum value of input array.

        SELECT array_max(ARRAY[-1,1,2]); -- 2
        SELECT array_max(ARRAY['a','b','c']); -- c

.. function:: array_min(x) -> x

    Returns the minimum value of input array.

        SELECT array_min(ARRAY[-1,1,2]); -- -1
        SELECT array_min(ARRAY['a','b','c']); -- a

.. function:: array_position(x, element) -> bigint

    Returns the position of the first occurrence of the ``element`` in array ``x`` (or 0 if not found).

        SELECT array_position(ARRAY[10,20,30,40,50],10); -- 1
        SELECT array_position(ARRAY[10,20,30,40,50],1); -- 0

.. function:: array_remove(x, element) -> array

    Remove all elements that equal ``element`` from array ``x``.

        SELECT array_remove(ARRAY[10,20,30,40,50],10); -- [20, 30, 40, 50]
        SELECT array_remove(ARRAY['a','b','c','d','e'],'z'); -- [a, b, c, d, e]

.. function:: array_sort(x) -> array

    Sorts and returns the array ``x``. The elements of ``x`` must be orderable.
    Null elements will be placed at the end of the returned array.

        SELECT array_sort(ARRAY['e','d','c','b','a']); -- [a, b, c, d, e]
        SELECT array_sort(ARRAY['e',null,'c','b','a']); -- [a, b, c, e, null]

.. function:: arrays_overlap(x, y) -> boolean

    Tests if arrays ``x`` and ``y`` have any any non-null elements in common.
    Returns null if there are no non-null elements in common but either array contains null.

.. function:: cardinality(x) -> bigint

    Returns the cardinality (size) of the array ``x``.

        SELECT cardinality(ARRAY[10,20,30,40,50],10); -- 5
        SELECT cardinality(ARRAY['e',null,'c','b','a']); -- 5

.. function:: concat(array1, array2, ..., arrayN) -> array
    :noindex:

    Concatenates the arrays ``array1``, ``array2``, ``...``, ``arrayN``.
    This function provides the same functionality as the SQL-standard concatenation operator (``||``).

        SELECT concat(ARRAY[1,1,2], ARRAY[2,3]); --  [1, 1, 2, 2, 3]
        SELECT concat(ARRAY[1,1,2], ARRAY[2,null]); -- [1, 1, 2, 2, null]
        SELECT concat(ARRAY['a','a','b'], ARRAY['b', 'c']); -- ['a', 'a', 'b', 'b', 'c']

.. function:: contains(x, element) -> boolean

    Returns true if the array ``x`` contains the ``element``.

        SELECT contains(ARRAY[1,1,2], 3); -- false
        SELECT contains(ARRAY['a',null,'b'], 'a'); -- true
        SELECT contains(ARRAY['a',null,'b'], null); -- NULL

.. function:: element_at(array<E>, index) -> E

    Returns element of ``array`` at given ``index``.
    If ``index`` >= 0, this function provides the same functionality as the SQL-standard subscript operator (``[]``).
    If ``index`` < 0, ``element_at`` accesses elements from the last to the first.

        SELECT element_at(ARRAY[10,20,30,40,50],1); -- 10
        SELECT element_at(ARRAY[10,20,30,40,50],-3); -- 30
        SELECT element_at(ARRAY[10,20,30,40,50],10); -- NULL
        SELECT element_at(ARRAY[10,20,30,40,50],-10); -- NULL

.. function:: filter(array, function) -> array
    :noindex:

    See :func:`filter`.

.. function:: flatten(x) -> array

    Flattens an ``array(array(T))`` to an ``array(T)`` by concatenating the contained arrays.

        SELECT flatten(ARRAY[ARRAY[1,2,3],ARRAY[10,20,30]]); -- [1, 2, 3, 10, 20, 30]
        SELECT flatten(ARRAY[ARRAY[10,2,3],ARRAY[10,20,30]]); -- [10, 2, 3, 10, 20, 30]
        SELECT flatten(ARRAY[ARRAY[1,2,3],ARRAY[10,null,30]]); -- [1, 2, 3, 10, null, 30]

.. function:: reduce(array, initialState, inputFunction, outputFunction) -> x
    :noindex:

    See :func:`reduce`.

.. function:: reverse(x) -> array
    :noindex:

    Returns an array which has the reversed order of array ``x``.

        SELECT reverse(ARRAY[10,20,30,40,50]); -- [50, 40, 30, 20, 10]
        SELECT reverse(ARRAY[10,null,30,40,50]); -- [50, 40, 30, null, 10]

.. function:: sequence(start, stop) -> array<bigint>

    Generate a sequence of integers from ``start`` to ``stop``, incrementing
    by ``1`` if ``start`` is less than or equal to ``stop``, otherwise ``-1``.

.. function:: sequence(start, stop, step) -> array<bigint>

    Generate a sequence of integers from ``start`` to ``stop``, incrementing by ``step``.

.. function:: sequence(start, stop, step) -> array<timestamp>

    Generate a sequence of timestamps from ``start`` to ``stop``, incrementing by ``step``.
    The type of ``step`` can be either ``INTERVAL DAY TO SECOND`` or ``INTERVAL YEAR TO MONTH``.

.. function:: shuffle(x) -> array

    Generate a random permutation of the given array ``x``.

.. function:: slice(x, start, length) -> array

    Subsets array ``x`` starting from index ``start`` (or starting from the end
    if ``start`` is negative) with a length of ``length``.

        SELECT slice(ARRAY[1,2,3,4,5], 2, 2); -- [2, 3]
        SELECT slice(ARRAY[1,2,3,4,5], -3, 2); -- [3, 4]
        SELECT slice(ARRAY[1,2,3,4,5], -10, 2); -- []

.. function:: transform(array, function) -> array
    :noindex:

    See :func:`transform`.

.. function:: zip(array1, array2[, ...]) -> array<row>

    Merges the given arrays, element-wise, into a single array of rows. The M-th element of
    the N-th argument will be the N-th field of the M-th output element.
    If the arguments have an uneven length, missing values are filled with ``NULL``. ::

        SELECT zip(ARRAY[1, 2], ARRAY['1b', null, '3b']); -- [ROW(1, '1b'), ROW(2, null), ROW(null, '3b')]
        SELECT zip(ARRAY[1,2,3],ARRAY[10,20,30]); -- [ROW(1, 10), ROW(2, 20), ROW(3, 30)]

.. function:: zip_with(array1, array2, function) -> array
    :noindex:

    See :func:`zip_with`.
