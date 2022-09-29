=============================
Array Functions
=============================

.. function:: array_distinct(array(E)) -> array(E)

    Remove duplicate values from the input array. ::

        SELECT array_distinct(ARRAY [1, 2, 3]); -- [1, 2, 3]
        SELECT array_distinct(ARRAY [1, 2, 1]); -- [1, 2]
        SELECT array_distinct(ARRAY [1, NULL, NULL]); -- [1, NULL]

.. function:: array_duplicates(array(E)) -> array(E)

    Returns a set of elements that occur more than once in array.
    E must be bigint or varchar.

        select array_duplicates(ARRAY [5, 2, 5, 1, 1, 5, null, null])); -- [null, 1, 5]

.. function:: array_except(array(E) x, array(E) y) -> array(E)

    Returns an array of the elements in array ``x`` but not in array ``y``, without duplicates. ::

        SELECT array_except(ARRAY [1, 2, 3], ARRAY [4, 5, 6]); -- [1, 2, 3]
        SELECT array_except(ARRAY [1, 2, 3], ARRAY [1, 2]); -- [3]
        SELECT array_except(ARRAY [1, 2, 2], ARRAY [1, 1, 2]); -- []
        SELECT array_except(ARRAY [1, 2, 2], ARRAY [1, 3, 4]); -- [2]
        SELECT array_except(ARRAY [1, NULL, NULL], ARRAY [1, 1, NULL]); -- []

.. function:: array_intersect(array(E) x, array(E) y) -> array(E)

    Returns an array of the elements in the intersection of array ``x`` and array ``y``, without duplicates. ::

        SELECT array_intersect(ARRAY [1, 2, 3], ARRAY[4, 5, 6]); -- []
        SELECT array_intersect(ARRAY [1, 2, 2], ARRAY[1, 1, 2]); -- [1, 2]
        SELECT array_intersect(ARRAY [1, NULL, NULL], ARRAY[1, 1, NULL]); -- [1, NULL]

.. function:: array_join(x, delimiter, null_replacement) -> varchar

    Concatenates the elements of the given array using the delimiter and an optional string to replace nulls. ::

        SELECT array_join(ARRAY [1, 2, 3], ",") -- "1,2,3"
        SELECT array_join(ARRAY [1, NULL, 2], ",") -- "1,2"
        SELECT array_join(ARRAY [1, NULL, 2], ",", "0") -- "1,0,2"

.. function:: array_max(array(E)) -> E

    Returns the maximum value of input array. ::

        SELECT array_max(ARRAY [1, 2, 3]); -- 3
        SELECT array_max(ARRAY [-1, -2, -2]); -- -1
        SELECT array_max(ARRAY [-1, -2, NULL]); -- NULL
        SELECT array_max(ARRAY []); -- NULL

.. function:: array_min(array(E)) -> E

    Returns the minimum value of input array. ::

        SELECT array_min(ARRAY [1, 2, 3]); -- 1
        SELECT array_min(ARRAY [-1, -2, -2]); -- -2
        SELECT array_min(ARRAY [-1, -2, NULL]); -- NULL
        SELECT array_min(ARRAY []); -- NULL

.. function:: arrays_overlap(x, y) -> boolean

    Tests if arrays ``x`` and ``y`` have any non-null elements in common.
    Returns null if there are no non-null elements in common but either array contains null.

.. function:: array_position(x, element) -> bigint

    Returns the position of the first occurrence of the ``element`` in array ``x`` (or 0 if not found).

.. function:: array_position(x, element, instance) -> bigint

    If ``instance > 0``, returns the position of the ``instance``-th occurrence of the ``element`` in array ``x``. If ``instance < 0``, returns the position of the ``instance``-to-last occurrence of the ``element`` in array ``x``. If no matching element instance is found, 0 is returned.

.. function:: array_sort(array(E)) -> array(E)

     Returns an array which has the sorted order of the input array x. The elements of x must
     be orderable. Null elements will be placed at the end of the returned array.

        SELECT array_sort(ARRAY [1, 2, 3]); -- [1, 2, 3]
        SELECT array_sort(ARRAY [3, 2, 1]); -- [1, 2, 3]
        SELECT array_sort(ARRAY [2, 1, NULL]; -- [1, 2, NULL]
        SELECT array_sort(ARRAY [NULL, 1, NULL]); -- [1, NULL, NULL]
        SELECT array_sort(ARRAY [NULL, 2, 1]); -- [1, 2, NUL]

.. function:: array_sum(array(T)) -> bigint/double

    Returns the sum of all non-null elements of the array. If there is no non-null elements, returns 0. The behaviour is similar to aggregation function sum().
    T must be coercible to double. Returns bigint if T is coercible to bigint. Otherwise, returns double.

.. function:: cardinality(x) -> bigint

    Returns the cardinality (size) of the array ``x``.

.. function:: combinations(array(T), n) -> array(array(T))

    Returns ``n``- element combinations of the input ``array``. If the input array has no duplicates, combinations returns ``n``- element subsets. Order of subgroup is deterministic but unspecified. Order of elements within a subgroup are deterministic but unspecified. ``n`` must not be greater than 5, and the total size of subgroups generated must be smaller than 100000. ::

        SELECT combinations(ARRAY['foo', 'bar', 'boo'],2); --[['foo', 'bar'], ['foo', 'boo']['bar', 'boo']]
        SELECT combinations(ARRAY[1,2,3,4,5],3); --[[1,2,3], [1,2,4], [1,3,4], [2,3,4]]
        SELECT combinations(ARRAY[1,2,2],2); --[[1,2],[1,2],[2,2]]

.. function:: contains(x, element) -> boolean

    Returns true if the array ``x`` contains the ``element``.

.. function:: element_at(array(E), index) -> E

    Returns element of ``array`` at given ``index``.
    If ``index`` > 0, this function provides the same functionality as the SQL-standard subscript operator (``[]``).
    If ``index`` < 0, ``element_at`` accesses elements from the last to the first.

.. function:: filter(array(T), function(T,boolean)) -> array(T)

    Constructs an array from those elements of ``array`` for which ``function`` returns true::

        SELECT filter(ARRAY [], x -> true); -- []
        SELECT filter(ARRAY [5, -6, NULL, 7], x -> x > 0); -- [5, 7]
        SELECT filter(ARRAY [5, NULL, 7, NULL], x -> x IS NOT NULL); -- [5, 7]

.. function:: reduce(array(T), initialState S, inputFunction(S,T,S), outputFunction(S,R)) -> R

    Returns a single value reduced from ``array``. ``inputFunction`` will
    be invoked for each element in ``array`` in order. In addition to taking
    the element, ``inputFunction`` takes the current state, initially
    ``initialState``, and returns the new state. ``outputFunction`` will be
    invoked to turn the final state into the result value. It may be the
    identity function (``i -> i``). ::

        SELECT reduce(ARRAY [], 0, (s, x) -> s + x, s -> s); -- 0
        SELECT reduce(ARRAY [5, 20, 50], 0, (s, x) -> s + x, s -> s); -- 75
        SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> s + x, s -> s); -- NULL
        SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> s + COALESCE(x, 0), s -> s); -- 75
        SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s); -- 75
        SELECT reduce(ARRAY [2147483647, 1], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); -- 2147483648
        SELECT reduce(ARRAY [5, 6, 10, 20], -- calculates arithmetic average: 10.25
                      CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
                      (s, x) -> CAST(ROW(x + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
                      s -> IF(s.count = 0, NULL, s.sum / s.count));

.. function:: reverse(array(E)) -> array(E)

    Returns an array which has the reversed order of the input array.

.. function:: slice(array(E), start, length) -> array(E)

    Returns a subarray starting from index ``start``(or starting from the end
    if ``start`` is negative) with a length of ``length``.

.. function:: subscript(array(E), index) -> E

    Returns element of ``array`` at given ``index``. The index starts from one.
    Throws if the element is not present in the array. Corresponds to SQL subscript operator [].

    SELECT my_array[1] AS first_element

.. function:: transform(array(T), function(T,U)) -> array(U)

    Returns an array that is the result of applying ``function`` to each element of ``array``::

        SELECT transform(ARRAY [], x -> x + 1); -- []
        SELECT transform(ARRAY [5, 6], x -> x + 1); -- [6, 7]
        SELECT transform(ARRAY [5, NULL, 6], x -> COALESCE(x, 0) + 1); -- [6, 1, 7]
        SELECT transform(ARRAY ['x', 'abc', 'z'], x -> x || '0'); -- ['x0', 'abc0', 'z0']
        SELECT transform(ARRAY [ARRAY [1, NULL, 2], ARRAY[3, NULL]], a -> filter(a, x -> x IS NOT NULL)); -- [[1, 2], [3]]

.. function:: zip(array(T), array(U),..) -> array(row(T,U, ...))

    Returns the merge of the given arrays, element-wise into a single array of rows.
    The M-th element of the N-th argument will be the N-th field of the M-th output element.
    If the arguments have an uneven length, missing values are filled with ``NULL`` ::

    SELECT zip(ARRAY[1, 2], ARRAY['1b', null, '3b']); -- [ROW(1, '1b'), ROW(2, null), ROW(null, '3b')]

.. function:: zip_with(array(T), array(U), function(T,U,R)) -> array(R)

    Merges the two given arrays, element-wise, into a single array using ``function``.
    If one array is shorter, nulls are appended at the end to match the length of the longer array, before applying ``function``::

        SELECT zip_with(ARRAY[1, 3, 5], ARRAY['a', 'b', 'c'], (x, y) -> (y, x)); -- [ROW('a', 1), ROW('b', 3), ROW('c', 5)]
        SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4], (x, y) -> x + y); -- [4, 6]
        SELECT zip_with(ARRAY['a', 'b', 'c'], ARRAY['d', 'e', 'f'], (x, y) -> concat(x, y)); -- ['ad', 'be', 'cf']
        SELECT zip_with(ARRAY['a'], ARRAY['d', null, 'f'], (x, y) -> coalesce(x, y)); -- ['a', null, 'f']

