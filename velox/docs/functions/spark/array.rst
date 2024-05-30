=============================
Array Functions
=============================

.. spark:function:: aggregate(array(E), start, merge, finish) -> array(E)

    Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state.
    The final state is converted into the final result by applying a finish function. ::

        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10); -- 60

.. spark:function:: array(E, E1, ..., En) -> array(E, E1, ..., En)

    Returns an array with the given elements. ::

        SELECT array(1, 2, 3); -- [1,2,3]

.. spark:function:: array_contains(array(E), value) -> boolean

    Returns true if the array contains the value. ::

        SELECT array_contains(array(1, 2, 3), 2); -- true

.. spark:function:: array_distinct(array(E)) -> array(E)

    Remove duplicate values from the input array. ::

        SELECT array_distinct(ARRAY [1, 2, 3]); -- [1, 2, 3]
        SELECT array_distinct(ARRAY [1, 2, 1]); -- [1, 2]
        SELECT array_distinct(ARRAY [1, NULL, NULL]); -- [1, NULL]

.. spark:function:: array_except(array(E) x, array(E) y) -> array(E)

    Returns an array of the elements in array ``x`` but not in array ``y``, without duplicates. ::

        SELECT array_except(ARRAY [1, 2, 3], ARRAY [4, 5, 6]); -- [1, 2, 3]
        SELECT array_except(ARRAY [1, 2, 3], ARRAY [1, 2]); -- [3]
        SELECT array_except(ARRAY [1, 2, 2], ARRAY [1, 1, 2]); -- []
        SELECT array_except(ARRAY [1, 2, 2], ARRAY [1, 3, 4]); -- [2]
        SELECT array_except(ARRAY [1, NULL, NULL], ARRAY [1, 1, NULL]); -- []

.. spark:function:: array_intersect(array(E), array(E1)) -> array(E2)

    Returns an array of the elements in the intersection of array1 and array2, without duplicates. ::

        SELECT array_intersect(array(1, 2, 3), array(1, 3, 5)); -- [1,3]

.. spark:function:: array_max(array(E)) -> E

    Returns maximum non-NULL element of the array. Returns NULL if array is empty or all elements are NULL.
    When E is DOUBLE or REAL, returns NaN if any element is NaN. ::

        SELECT array_max(array(1, 2, 3)); -- 3
        SELECT array_max(array(-1, -2, -2)); -- -1
        SELECT array_max(array(-1, -2, NULL)); -- -1
        SELECT array_max(array()); -- NULL
        SELECT array_max(array(-0.0001, -0.0002, -0.0003, float('nan'))); -- NaN

.. spark:function:: array_min(array(E)) -> E

    Returns minimum non-NULL element of the array. Returns NULL if array is empty or all elements are NULL.
    When E is DOUBLE or REAL, NaN value is considered greater than any non-NaN value. ::

        SELECT array_min(array(1, 2, 3）); -- 1
        SELECT array_min(array(-1, -2, -2）); -- -2
        SELECT array_min(array(-1, -2, NULL)); -- -2
        SELECT array_min(array(NULL, NULL)); -- NULL
        SELECT array_min(array()); -- NULL
        SELECT array_min(array(4.0, float('nan')]); -- 4.0
        SELECT array_min(array(NULL, float('nan'))); -- NaN

.. spark:function:: array_position(x, element) -> bigint

    Returns the position (1-based) of the first occurrence of the ``element`` in array ``x`` (or 0 if not found). ::

        SELECT array_position(array(1, 2, 3), 2); -- 2
        SELECT array_position(array(1, 2, 3), 4); -- 0
        SELECT array_position(array(1, 2, 3, 2), 2); -- 2

.. spark:function:: array_remove(x, element) -> array

    Remove all elements that equal ``element`` from array ``x``. Returns NULL as result if ``element`` is NULL.
    If array ``x`` is empty array, returns empty array. If all elements in array ``x`` are NULL but ``element`` is not NULL,
    returns array ``x``. ::

        SELECT array_remove(array(1, 2, 3), 3); -- [1, 2]
        SELECT array_remove(array(2, 1, NULL), 1); -- [2, NULL]
        SELECT array_remove(array(1, 2, NULL), NULL); -- NULL
        SELECT array_remove(array(), 1); -- []
        SELECT array_remove(array(NULL, NULL), -1); -- [NULL, NULL]

.. spark:function:: array_repeat(element, count) -> array(E)

    Returns an array containing ``element`` ``count`` times. If ``count`` is negative or zero,
    returns empty array. If ``element`` is NULL, returns an array containing ``count`` NULLs.
    If ``count`` is NULL, returns NULL as result. Throws an exception if ``count`` exceeds 10'000. ::

        SELECT array_repeat(100, 3); -- [100, 100, 100]
        SELECT array_repeat(NULL, 3); -- [NULL, NULL, NULL]
        SELECT array_repeat(100, NULL); -- NULL
        SELECT array_repeat(100, 0); -- []
        SELECT array_repeat(100, -1); -- []

.. spark:function:: array_size(array(E)) -> integer

        Returns the size of the array. ::

            SELECT array_size(array(1, 2, 3)); -- 3

.. spark:function:: array_sort(array(E)) -> array(E)

    Returns an array which has the sorted order of the input array(E). The elements of array(E) must
    be orderable. Null elements will be placed at the end of the returned array. ::

        SELECT array_sort(array(1, 2, 3)); -- [1, 2, 3]
        SELECT array_sort(array(3, 2, 1)); -- [1, 2, 3]
        SELECT array_sort(array(2, 1, NULL); -- [1, 2, NULL]
        SELECT array_sort(array(NULL, 1, NULL)); -- [1, NULL, NULL]
        SELECT array_sort(array(NULL, 2, 1)); -- [1, 2, NULL]

.. spark:function:: concat(array(E), array(E1), ..., array(En)) -> array(E, E1, ..., En)

    Returns the concatenation of array(E), array(E1), ..., array(En). ::

        SELECT concat(array(1, 2, 3), array(4, 5), array(6)); -- [1, 2, 3, 4, 5, 6]

.. spark:function:: filter(array(E), func) -> array(E)

    Filters the input array using the given predicate. ::

        SELECT filter(array(1, 2, 3), x -> x % 2 == 1); -- [1, 3]
        SELECT filter(array(0, 2, 3), (x, i) -> x > i); -- [2, 3]
        SELECT filter(array(0, null, 2, 3, null), x -> x IS NOT NULL); -- [0, 2, 3]

.. function:: flatten(array(array(E))) -> array(E)

    Transforms an array of arrays into a single array.
    Returns NULL if the input is NULL or any of the nested arrays is NULL. ::

        SELECT flatten(array(array(1, 2), array(3, 4))); -- [1, 2, 3, 4]
        SELECT flatten(array(array(1, 2), array(3, NULL))); -- [1, 2, 3, NULL]
        SELECT flatten(array(array(1, 2), NULL, array(3, 4))); -- NULL

.. spark:function:: get(array(E), index) -> E

    Returns an element of the array at the specified 0-based index.
    Returns NULL if index points outside of the array boundaries. ::

        SELECT get(array(1, 2, 3), 0); -- 1
        SELECT get(array(1, 2, 3), 3); -- NULL
        SELECT get(array(1, 2, 3), -1); -- NULL
        SELECT get(array(1, 2, 3), NULL); -- NULL
        SELECT get(array(1, 2, NULL), 2); -- NULL

.. spark:function:: in(value, array(E)) -> boolean

    Returns true if value matches at least one of the elements of the array.
    Supports BOOLEAN, REAL, DOUBLE, BIGINT, VARCHAR, TIMESTAMP, DATE input types.

.. spark:function:: shuffle(array(E), seed) -> array(E)

    Generates a random permutation of the given ``array`` using a seed derived 
    from the parameter ``seed`` and the configuration `spark.partition_id`.
    ``seed`` must be constant. ::

        SELECT shuffle(array(1, 2, 3), 0); -- [3, 1, 2]
        SELECT shuffle(array(0, 0, 0), 0); -- [0, 0, 0]
        SELECT shuffle(array(1, NULL, 1, NULL, 2), 0); -- [2, 1, NULL, NULL, 1]

.. spark:function:: size(array(E)) -> bigint

    Returns the size of the array. Returns null for null input
    if :doc:`spark.legacy_size_of_null <../../configs>` is set to false.
    Otherwise, returns -1 for null input.

.. spark:function:: sort_array(array(E)) -> array(E)

    Returns an array which has the sorted order of the input array. The elements of array must
    be orderable. Null elements will be placed at the beginning of the returned array. ::

        SELECT sort_array(array(1, 2, 3)); -- [1, 2, 3]
        SELECT sort_array(array(NULL, 2, 1)); -- [NULL, 1, 2]

.. spark:function:: sort_array(array(E), ascendingOrder) -> array(E)
   :noindex:

    Returns an array which has the sorted order of the input array. The elements of array must
    be orderable. Null elements will be placed at the beginning of the returned array in ascending
    order or at the end of the returned array in descending order. ::

        SELECT sort_array(array(3, 2, 1), true); -- [1, 2, 3]
        SELECT sort_array(array(2, 1, NULL, true); -- [NULL, 1, 2]
        SELECT sort_array(array(NULL, 1, NULL), false); -- [1, NULL, NULL]

.. spark:function:: transform(array(E), function) -> array(E)

    Transforms elements in an array using the function. ::

        SELECT transform(array(1, 2, 3), x -> x + 1); -- [2,3,4]
        SELECT transform(array(1, 2, 3), (x, i) -> x + i); -- [1,3,5]
