===============
Array Functions
===============

.. spark:function:: aggregate(array(E), start, merge, finish) -> array(E)

    Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state.
    The final state is converted into the final result by applying a finish function. ::

        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10); -- 60

.. spark:function:: array(E, E1, ..., En) -> array(E, E1, ..., En)

    Returns an array with the given elements. ::

        SELECT array(1, 2, 3); -- [1,2,3]

.. spark:function:: array_append(array(E), element) -> array(E)

    Add the ``element`` at the end of the input ``array``.
    Type of ``element`` should be the same to the type of elements in the ``array``.
    NULL element is also appended into the ``array``. Returns NULL when the input ``array`` is NULL. ::

        SELECT array_append(array(1, 2, 3), 2); -- [1, 2, 3, 2]
        SELECT array_append(array(1, 2, 3), NULL); -- [1, 2, 3, NULL]

.. spark:function:: array_compact(array(E) x) -> array(E)

    Removes all NULL elements from array ``x``. Returns NULL if array ``x`` is NULL.
    Returns empty array if array ``x`` is empty or all elements in it are NULL. ::

        SELECT array_compact(array(1, 2, NULL, 3)); -- [1, 2, 3]
        SELECT array_compact(array()); -- []
        SELECT array_compact(array(NULL)); -- []
        SELECT array_compact(NULL); -- NULL
        SELECT array_compact(array(array(1, 2), NULL, array(NULL, 3, 4))); -- [[1, 2], [NULL, 3, 4]]

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

.. spark:function:: array_insert(array(E), pos, E, legacyNegativeIndex) -> array(E)

    Places new element into index ``pos`` of the input ``array``. Returns NULL if the input ``array`` or
    ``pos`` is NULL. Array indices are 1-based and exception is thrown when ``pos`` is 0. The maximum
    negative index is -1. When ``legacyNegativeIndex`` is true, -1 points to the last but one position.
    Otherwise, -1 points to the last position. Index above array size appends the array or prepends the
    array if index is negative, with 'null' elements. ::

        SELECT array_insert(NULL, 1, 0, false); -- NULL
        SELECT array_insert(NULL, 1, 0, true); -- NULL
        SELECT array_insert(array(1, 2), NULL, 0, false); -- NULL
        SELECT array_insert(array(1, 2), NULL, 0, true); -- NULL
        SELECT array_insert(array(1, 2), 1, 0, false); -- [0, 1, 2]
        SELECT array_insert(array(1, 2), 1, 0, true); -- [0, 1, 2]
        SELECT array_insert(array(1, 2), 4, 0, false); -- [1, 2, NULL, 0]
        SELECT array_insert(array(1, 2), 4, 0, true); -- [1, 2, NULL, 0]
        SELECT array_insert(array(1, 2), -1, 0, false); -- [1, 2, 0]
        SELECT array_insert(array(1, 2), -1, 0, true); -- [1, 0, 2]
        SELECT array_insert(array(1, 2), -4, 0, false); -- [0, NULL, 1, 2]
        SELECT array_insert(array(1, 2), -4, 0, true); -- [0, NULL, NULL, 1, 2]

.. spark:function:: array_intersect(array(E), array(E1)) -> array(E2)

    Returns an array of the elements in the intersection of array1 and array2, without duplicates. ::

        SELECT array_intersect(array(1, 2, 3), array(1, 3, 5)); -- [1,3]

.. spark:function:: array_join(x, delimiter[, nullReplacement]) -> varchar

    Concatenates the elements of the given array using the ``delimiter`` and an optional string to replace nulls.
    If no value is set for ``nullReplacement``, any null value is filtered. ::

        SELECT array_join(array('1', '2', '3'), ',') -- '1,2,3'
        SELECT array_join(array('1', NULL, '2'), ',') -- '1,2'
        SELECT array_join(array('1', NULL, '2'), ',', '0') -- '1,0,2'

.. spark:function:: array_max(array(E)) -> E

    Returns maximum non-NULL element of the array. Returns NULL if array is empty or all elements are NULL.
    When E is DOUBLE or REAL, returns NaN if any element is NaN. ::

        SELECT array_max(array(1, 2, 3)); -- 3
        SELECT array_max(array(-1, -2, -2)); -- -1
        SELECT array_max(array(-1, -2, NULL)); -- -1
        SELECT array_max(array()); -- NULL
        SELECT array_max(array(-0.0001, -0.0002, -0.0003, float('nan'))); -- NaN
        SELECT array_max(array(array(1), array(NULL))); -- array(1)
        SELECT array_max(array(array(1), array(2, 1), array(2))); -- array(2, 1)
        SELECT array_max(array(array(1.0), array(1.0, 2.0), array(cast('NaN' as double)))); --array(NaN)

.. spark:function:: array_min(array(E)) -> E

    Returns minimum non-NULL element of the array. Returns NULL if array is empty or all elements are NULL.
    When E is DOUBLE or REAL, NaN value is considered greater than any non-NaN value. ::

        SELECT array_min(array(1, 2, 3)); -- 1
        SELECT array_min(array(-1, -2, -2)); -- -2
        SELECT array_min(array(-1, -2, NULL)); -- -2
        SELECT array_min(array(NULL, NULL)); -- NULL
        SELECT array_min(array()); -- NULL
        SELECT array_min(array(4.0, float('nan'))); -- 4.0
        SELECT array_min(array(NULL, float('nan'))); -- NaN
        SELECT array_min(array(array(1), array(NULL))); -- array(NULL)
        SELECT array_min(array(array(1), array(1, 2), array(2))); -- array(1)
        SELECT array_min(array(array(1.0), array(1.0, 2.0), array(cast('NaN' as double)))); --array(1.0)

.. spark:function:: array_position(x, element) -> bigint

    Returns the position (1-based) of the first occurrence of the ``element`` in array ``x`` (or 0 if not found). ::

        SELECT array_position(array(1, 2, 3), 2); -- 2
        SELECT array_position(array(1, 2, 3), 4); -- 0
        SELECT array_position(array(1, 2, 3, 2), 2); -- 2

.. spark:function:: array_prepend(x, element) -> array

    Add the ``element`` at the beginning of the input array ``x``.
    Type of ``element`` should be the same to the type of elements in the array ``x``.
    NULL element is also prepended into the array ``x``. Returns NULL when the input array ``x`` is NULL. ::

        SELECT array_prepend(array(1, 2, 3), 2); -- [2, 1, 2, 3]
        SELECT array_prepend(array(1, 2, 3), NULL); -- [NULL, 1, 2, 3]
        SELECT array_prepend(NULL, 1); -- NULL
        SELECT array_prepend(array(NULL, 2, 3), 1); -- [1, NULL, 2, 3]

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

.. spark:function:: array_sort(array(E)) -> array(E)

    Returns an array which has the sorted order of the input array(E). The elements of array(E) must
    be orderable. NULL and NaN elements will be placed at the end of the returned array, with NaN elements appearing before NULL elements for floating-point types. ::

        SELECT array_sort(array(1, 2, 3)); -- [1, 2, 3]
        SELECT array_sort(array(3, 2, 1)); -- [1, 2, 3]
        SELECT array_sort(array(2, 1, NULL)); -- [1, 2, NULL]
        SELECT array_sort(array(NULL, 1, NULL)); -- [1, NULL, NULL]
        SELECT array_sort(array(NULL, 2, 1)); -- [1, 2, NULL]
        SELECT array_sort(array(4.0, NULL, float('nan'), 3.0)); -- [3.0, 4.0, NaN, NULL]
        SELECT array_sort(array(array(), array(1, 3, NULL), array(NULL, 6), NULL, array(2, 1))); -- [[], [NULL, 6], [1, 3, NULL], [2, 1], NULL]

.. spark:function:: array_sort(array(E), function(E,U)) -> array(E)
    :noindex:

    Returns the array sorted by values computed using specified lambda in ascending order. ``U`` must be an orderable type.
    NULL and NaN elements returned by the lambda function will be placed at the end of the returned array, with NaN elements appearing before NULL elements.
    This function is not supported in Spark and is only used inside Velox for rewriting :spark:func:`array_sort(array(E), function(E,E,U)) -> array(E)` as :spark:func:`array_sort(array(E), function(E,U)) -> array(E)`. ::

.. spark:function:: array_sort(array(E), function(E,E,U)) -> array(E)
    :noindex:

    Returns the array sorted by values computed using specified lambda in ascending
    order. ``U`` must be an orderable type.
    The function attempts to analyze the lambda function and rewrite it into a simpler call that
    specifies the sort-by expression (like :spark:func:`array_sort(array(E), function(E,U)) -> array(E)`). For example, ``(left, right) -> if(length(left) > length(right), 1, if(length(left) < length(right), -1, 0))`` will be rewritten to ``x -> length(x)``. If rewrite is not possible, a user error will be thrown.
    If the rewritten function returns NULL, the corresponding element will be placed at the end the returned array. Please note that due to this rewrite optimization, the NULL handling logics between Spark and Velox differ. In Spark, the position of NULL element is determined by the comparison of NULL with other elements. ::

        SELECT array_sort(array('cat', 'leopard', 'mouse'), (left, right) -> if(length(left) > length(right), 1, if(length(left) < length(right), -1, 0))); -- ['cat', 'mouse', 'leopard']
        select array_sort(array("abcd123", "abcd", NULL, "abc"), (left, right) -> if(length(left) > length(right), 1, if(length(left) < length(right), -1, 0))); -- ["abc", "abcd", "abcd123", NULL]
        select array_sort(array("abcd123", "abcd", NULL, "abc"), (left, right) -> if(length(left) > length(right), 1, if(length(left) = length(right), 0, -1))); -- ["abc", "abcd", "abcd123", NULL] different with Spark: ["abc", NULL, "abcd", "abcd123"]

.. spark:function:: array_union(array(E) x, array(E) y) -> array(E)

    Returns an array of the elements in the union of ``x`` and ``y``, without duplicates. ::

        SELECT array_union(array(1, 2, 3), array(1, 3, 5)); -- [1, 2, 3, 5]
        SELECT array_union(array(1, 3, 5), array(1, 2, 3)); -- [1, 3, 5, 2]
        SELECT array_union(array(1, 2, 3), array(1, 3, 5, null)); -- [1, 2, 3, 5, null]
        SELECT array_union(array(1, 2, float('nan')), array(1, 3, float('nan'))); -- [1, 2, NaN, 3]
        SELECT array_union(array(array(1)), array(array(null))); -- [[1], [null]]

.. spark::function:: arrays_zip(array(T), array(U),..) -> array(row(T,U, ...))

    Returns the merge of the given arrays, element-wise into a single array of rows.
    The M-th element of the N-th argument will be the N-th field of the M-th output element.
    If the arguments have an uneven length, missing values are filled with ``NULL`` ::

        SELECT arrays_zip(ARRAY[1, 2], ARRAY['1b', null, '3b']); -- [ROW(1, '1b'), ROW(2, null), ROW(null, '3b')]

.. spark:function:: concat(array1, array2, ..., arrayN) -> array

    Concatenates the arrays ``array1``, ``array2``, ..., ``arrayN``. All parameters have the same type.
    This function provides the same functionality as the SQL-standard concatenation operator (``||``).
    Fails if the result array size exceeds INT_MAX - 15. ::

        SELECT concat(array(1, 2, 3), array(4, 5), array(6)); -- [1, 2, 3, 4, 5, 6]
        SELECT concat(array(1, 2, 3), null); -- NULL
        SELECT concat(array(1, 2), array(1, 2), array(1, null)); -- [1, 2, 1, 2, 1, NULL]
        SELECT concat(array(array(1, 2)), array(array(1, null))); -- [[1, 2], [1, NULL]]

.. spark:function:: exists(array(T), function(T, boolean)) → boolean

    Returns whether at least one element of an array matches the given predicate.

        Returns true if one or more elements match the predicate;
        Returns false if none of the elements matches (a special case is when the array is empty);
        Returns NULL if the predicate function returns NULL for one or more elements and false for all other elements.
        Throws an exception if the predicate fails for one or more elements and returns false or NULL for the rest.

.. spark:function:: filter(array(E), func) -> array(E)

    Filters the input array using the given predicate. ::

        SELECT filter(array(1, 2, 3), x -> x % 2 == 1); -- [1, 3]
        SELECT filter(array(0, 2, 3), (x, i) -> x > i); -- [2, 3]
        SELECT filter(array(0, null, 2, 3, null), x -> x IS NOT NULL); -- [0, 2, 3]

.. spark:function:: flatten(array(array(E))) -> array(E)

    Transforms an array of arrays into a single array.
    Returns NULL if the input is NULL or any of the nested arrays is NULL. ::

        SELECT flatten(array(array(1, 2), array(3, 4))); -- [1, 2, 3, 4]
        SELECT flatten(array(array(1, 2), array(3, NULL))); -- [1, 2, 3, NULL]
        SELECT flatten(array(array(1, 2), NULL, array(3, 4))); -- NULL

.. spark:function:: forall(array(T), function(T, boolean)) → boolean

    Returns whether all elements of an array match the given predicate.

        Returns true if all the elements match the predicate (a special case is when the array is empty);
        Returns false if one or more elements don't match;
        Returns NULL if the predicate function returns NULL for one or more elements and true for all other elements.
        Throws an exception if the predicate fails for one or more elements and returns true or NULL for the rest.

.. spark:function:: get(array(E), index) -> E

    Returns an element of the array at the specified 0-based ``index``.
    Returns NULL if ``index`` points outside of the array boundaries.
    ``index`` must be of an integral type. ::

        SELECT get(array(1, 2, 3), 0); -- 1
        SELECT get(array(1, 2, 3), 3); -- NULL
        SELECT get(array(1, 2, 3), -1); -- NULL
        SELECT get(array(1, 2, 3), NULL); -- NULL
        SELECT get(array(1, 2, NULL), 2); -- NULL

.. spark:function:: in(value, array(E)) -> boolean

    Returns true if value matches at least one of the elements of the array.
    Supports BOOLEAN, REAL, DOUBLE, BIGINT, VARCHAR, TIMESTAMP, DATE, DECIMAL input types.

.. spark:function:: shuffle(array(E), seed) -> array(E)

    Generates a random permutation of the given ``array`` using a seed derived
    from the parameter ``seed`` and the configuration `spark.partition_id`.
    ``seed`` must be constant. ::

        SELECT shuffle(array(1, 2, 3), 0); -- [3, 1, 2]
        SELECT shuffle(array(0, 0, 0), 0); -- [0, 0, 0]
        SELECT shuffle(array(1, NULL, 1, NULL, 2), 0); -- [2, 1, NULL, NULL, 1]

.. spark:function:: size(array(E), legacySizeOfNull) -> integer

    Returns the size of the array. Returns null for null input if `legacySizeOfNull`
    is set to false. Otherwise, returns -1 for null input. ::

        SELECT size(array(1, 2, 3), true); -- 3
        SELECT size(NULL, true); -- -1
        SELECT size(NULL, false); -- NULL

.. spark:function:: slice(array(E), start, length) -> array(E)

    Returns a subarray starting at 1-based index ``start`` or from end if negative, with ``length`` elements.
    Returns elements between ``start`` and the end of the array if ``start + length`` is outside of the array.
    Returns empty array if ``start`` point outside of the array or ``length`` is 0.
    Throws exception if ``start`` is 0 or ``length`` is negative. ::

        SELECT slice(array(1, 2, 3, 4), 2, 2); -- [2, 3]
        SELECT slice(array(1, 2, 3, 4), -2, 2); -- [3, 4]
        SELECT slice(array(1, 2, 3, 4), 5, 1); -- []
        SELECT slice(array(1, 2, 3, 4), 2, 5); -- [2, 3, 4]
        SELECT slice(array(1, 2, 3, 4), 2, 0); -- []
        SELECT slice(array(1, 2, 3, 4), 1, -1); -- error: The value of length argument of slice() function should not be negative
        SELECT slice(array(1, 2, 3, 4), 0, 1); -- error: SQL array indices start at 1

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

.. spark:function:: zip_with(array(T), array(U), function(T,U,R)) -> array(R)

    Merges the two given arrays, element-wise, into a single array using ``function``.
    If one array is shorter, nulls are appended at the end to match the length of the
    longer array, before applying ``function`` ::

        SELECT zip_with(ARRAY[1, 3, 5], ARRAY['a', 'b', 'c'], (x, y) -> (y, x)); -- [ROW('a', 1), ROW('b', 3), ROW('c', 5)]
        SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4], (x, y) -> x + y); -- [4, 6]
        SELECT zip_with(ARRAY['a', 'b', 'c'], ARRAY['d', 'e', 'f'], (x, y) -> concat(x, y)); -- ['ad', 'be', 'cf']
        SELECT zip_with(ARRAY['a'], ARRAY['d', null, 'f'], (x, y) -> coalesce(x, y)); -- ['a', null, 'f']
