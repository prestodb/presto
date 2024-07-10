=============================
Array Functions
=============================

.. function:: all_match(array(T), function(T, boolean)) → boolean

    Returns whether all elements of an array match the given predicate.

        Returns true if all the elements match the predicate (a special case is when the array is empty);
        Returns false if one or more elements don’t match;
        Returns NULL if the predicate function returns NULL for one or more elements and true for all other elements.
        Throws an exception if the predicate fails for one or more elements and returns true or NULL for the rest.

.. function:: any_match(array(T), function(T, boolean)) → boolean

    Returns whether at least one element of an array matches the given predicate.

        Returns true if one or more elements match the predicate;
        Returns false if none of the elements matches (a special case is when the array is empty);
        Returns NULL if the predicate function returns NULL for one or more elements and false for all other elements.
        Throws an exception if the predicate fails for one or more elements and returns false or NULL for the rest.

.. function:: none_match(array(T), function(T, boolean)) → boolean

    Returns whether no elements of an array match the given predicate.

        Returns true if none of the elements matches the predicate (a special case is when the array is empty);
        Returns false if one or more elements match;
        Returns NULL if the predicate function returns NULL for one or more elements and false for all other elements.
        Throws an exception if the predicate fails for one or more elements and returns false or NULL for the rest.

.. function:: array_average(array(double)) -> double

    Returns the average of all non-null elements of the array. If there are no non-null elements, returns null.

.. function:: array_cum_sum(array(T)) -> array(T)
    Returns the array whose elements are the cumulative sum of the input array, i.e. result[i] = input[1] + input[2] +
    … + input[i]. If there there is null elements in the array, the cumulative sum at and after the element is null. ::

        SELECT array_cum_sum(ARRAY [1, 2, 3]) -- array[1, 3, 6]
        SELECT array_cum_sum(ARRAY [1, 2, null, 3]) -- array[1, 3, null, null]

.. function:: array_distinct(array(E)) -> array(E)

    Remove duplicate values from the input array.
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal. ::

        SELECT array_distinct(ARRAY [1, 2, 3]); -- [1, 2, 3]
        SELECT array_distinct(ARRAY [1, 2, 1]); -- [1, 2]
        SELECT array_distinct(ARRAY [1, NULL, NULL]); -- [1, NULL]

.. function:: array_dupes(array(E)) -> boolean

    This is an alias for :func:`array_duplicates(array(E))`

.. function:: array_duplicates(array(E)) -> array(E)

    Returns a set of elements that occur more than once in array.
    E must be bigint or varchar.

        select array_duplicates(ARRAY [5, 2, 5, 1, 1, 5, null, null])); -- [null, 1, 5]

.. function:: array_except(array(E) x, array(E) y) -> array(E)

    Returns an array of the elements in array ``x`` but not in array ``y``, without duplicates.
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal. ::

        SELECT array_except(ARRAY [1, 2, 3], ARRAY [4, 5, 6]); -- [1, 2, 3]
        SELECT array_except(ARRAY [1, 2, 3], ARRAY [1, 2]); -- [3]
        SELECT array_except(ARRAY [1, 2, 2], ARRAY [1, 1, 2]); -- []
        SELECT array_except(ARRAY [1, 2, 2], ARRAY [1, 3, 4]); -- [2]
        SELECT array_except(ARRAY [1, NULL, NULL], ARRAY [1, 1, NULL]); -- []

.. function:: array_frequency(array(E) x) -> map(E, int)

    Returns a map: keys are the unique elements in the array, values are how many times the key appears.
    Ignores null elements. Empty array returns empty map. E must be bigint or varchar. ::

        SELECT array_frequency(ARRAY [1, 1, 2, 2, 2, 2]); -- {1 -> 2, 2 -> 4}
        SELECT array_frequency(ARRAY [1, 1, NULL, NULL, NULL]); -- {1 -> 2}
        SELECT array_frequency(ARRAY ["knock", "knock", "who", "?"]); -- {"knock" -> 2, "who" -> 1, "?" -> 1}
        SELECT array_frequency(ARRAY []); -- {}

.. function:: array_has_dupes(array(E)) -> boolean

    This is an alias for :func:`array_has_duplicates(array(E))`.

.. function:: array_has_duplicates(array(E)) -> boolean

    Returns a boolean: whether array has any elements that occur more than once.
    E must be bigint or varchar.

        select array_has_duplicates(ARRAY [5, 2, 5, 1, 1, 5, null, null])); -- true

.. function:: array_intersect(array(E) x, array(E) y) -> array(E)

    Returns an array of the elements in the intersection of array ``x`` and array ``y``, without duplicates.
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal. ::

        SELECT array_intersect(ARRAY [1, 2, 3], ARRAY[4, 5, 6]); -- []
        SELECT array_intersect(ARRAY [1, 2, 2], ARRAY[1, 1, 2]); -- [1, 2]
        SELECT array_intersect(ARRAY [1, NULL, NULL], ARRAY[1, 1, NULL]); -- [1, NULL]

.. function:: array_join(x, delimiter, null_replacement) -> varchar

    Concatenates the elements of the given array using the delimiter and an optional string to replace nulls. ::

        SELECT array_join(ARRAY [1, 2, 3], ",") -- "1,2,3"
        SELECT array_join(ARRAY [1, NULL, 2], ",") -- "1,2"
        SELECT array_join(ARRAY [1, NULL, 2], ",", "0") -- "1,0,2"

.. function:: array_max(array(E)) -> E

    Returns the maximum value of input array.
    NaN is considered to be greater than Infinity.
    Returns NULL if array contains a NULL value. ::

        SELECT array_max(ARRAY [1, 2, 3]); -- 3
        SELECT array_max(ARRAY [-1, -2, -2]); -- -1
        SELECT array_max(ARRAY [-1, -2, NULL]); -- NULL
        SELECT array_max(ARRAY []); -- NULL
        SELECT array_max(ARRAY [-1, nan(), NULL]); -- NULL
        SELECT array_max(ARRAY[{-1, -2, -3, nan()]); -- NaN
        SELECT array_max(ARRAY[{infinity(), nan()]); -- NaN

.. function:: array_min(array(E)) -> E

    Returns the minimum value of input array.
    NaN is considered to be greater than Infinity.
    Returns NULL if array contains a NULL value. ::

        SELECT array_min(ARRAY [1, 2, 3]); -- 1
        SELECT array_min(ARRAY [-1, -2, -2]); -- -2
        SELECT array_min(ARRAY [-1, -2, NULL]); -- NULL
        SELECT array_min(ARRAY []); -- NULL
        SELECT array_min(ARRAY [-1, nan(), NULL]); -- NULL
        SELECT array_min(ARRAY[{-1, -2, -3, nan()]); -- -1
        SELECT array_min(ARRAY[{infinity(), nan()]); -- Infinity

.. function:: array_normalize(array(E), E) -> array(E)

    Normalizes array ``x`` by dividing each element by the p-norm of the array. It is equivalent to ``TRANSFORM(array, v -> v / REDUCE(array, 0, (a, v) -> a + POW(ABS(v), p), a -> POW(a, 1 / p))``, but the reduce part is only executed once. Returns null if the array is null or there are null array elements. If ``p`` is 0, then the input array is returned. Only REAL and DOUBLE types are supported.

.. function:: arrays_overlap(x, y) -> boolean

    Tests if arrays ``x`` and ``y`` have any non-null elements in common.
    Returns null if there are no non-null elements in common but either array contains null.
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal.

.. function:: arrays_union(x, y) -> array

    Returns an array of the elements in the union of x and y, without duplicates.
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal.

.. function:: array_position(x, element) -> bigint

    Returns the position of the first occurrence of the ``element`` in array ``x`` (or 0 if not found).
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal.

.. function:: array_position(x, element, instance) -> bigint
    :noindex:

    If ``instance > 0``, returns the position of the ``instance``-th occurrence of the ``element`` in array ``x``. If ``instance < 0``, returns the position of the ``instance``-to-last occurrence of the ``element`` in array ``x``. If no matching element instance is found, 0 is returned.
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal.

.. function:: array_remove(x, element) -> array

    Remove all elements that equal ``element`` from array ``x``.
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal.

        SELECT array_remove(ARRAY [1, 2, 3], 3); -- [1, 2]
        SELECT array_remove(ARRAY [2, 1, NULL], 1); -- [2, NULL]
        SELECT array_remove(ARRAY [2.1, 1.1, nan()], nan()); -- [2.1, 1.1]

.. function:: array_sort(array(E)) -> array(E)

    Returns an array which has the sorted order of the input array x. E must be
    an orderable type. Null elements will be placed at the end of the returned array.
    May throw if E is and ARRAY or ROW type and input values contain nested nulls.
    Throws if deciding the order of elements would require comparing nested null values. ::

        SELECT array_sort(ARRAY [1, 2, 3]); -- [1, 2, 3]
        SELECT array_sort(ARRAY [3, 2, 1]); -- [1, 2, 3]
        SELECT array_sort(ARRAY [infinity(), -1.1, nan(), 1.1, -Infinity(), 0])); -- [-Infinity, -1.1, 0, 1.1, Infinity, NaN]
        SELECT array_sort(ARRAY [2, 1, NULL]; -- [1, 2, NULL]
        SELECT array_sort(ARRAY [NULL, 1, NULL]); -- [1, NULL, NULL]
        SELECT array_sort(ARRAY [NULL, 2, 1]); -- [1, 2, NULL]
        SELECT array_sort(ARRAY [ARRAY [1, 2], ARRAY [2, null]]); -- [[1, 2], [2, null]]
        SELECT array_sort(ARRAY [ARRAY [1, 2], ARRAY [1, null]]); -- failed: Ordering nulls is not supported

.. function:: array_sort(array(T), function(T,U)) -> array(T)
    :noindex:

    Returns the array sorted by values computed using specified lambda in ascending
    order. U must be an orderable type. Null elements will be placed at the end of
    the returned array. May throw if E is and ARRAY or ROW type and input values contain
    nested nulls. Throws if deciding the order of elements would require comparing nested
    null values. ::

        SELECT array_sort(ARRAY ['cat', 'leopard', 'mouse'], x -> length(x)); -- ['cat', 'mouse', 'leopard']

.. function:: array_sort_desc(array(E)) -> array(E)

    Returns the array sorted in the descending order. E must be an orderable type.
    Null elements will be placed at the end of the returned array.
    May throw if E is and ARRAY or ROW type and input values contain nested nulls.
    Throws if deciding the order of elements would require comparing nested null values. ::

        SELECT array_sort_desc(ARRAY [1, 2, 3]); -- [3, 2, 1]
        SELECT array_sort_desc(ARRAY [3, 2, 1]); -- [3, 2, 1]
        SELECT array_sort_desc(ARRAY [2, 1, NULL]; -- [2, 1, NULL]
        SELECT array_sort_desc(ARRAY [NULL, 1, NULL]); -- [1, NULL, NULL]
        SELECT array_sort_desc(ARRAY [NULL, 2, 1]); -- [2, 1, NULL]
        SELECT array_sort(ARRAY [ARRAY [1, 2], ARRAY [2, null]]); -- [[1, 2], [2, null]]
        SELECT array_sort(ARRAY [ARRAY [1, 2], ARRAY [1, null]]); -- failed: Ordering nulls is not supported

.. function:: array_sort_desc(array(T), function(T,U)) -> array(T)
    :noindex:

    Returns the array sorted by values computed using specified lambda in descending
    order. U must be an orderable type. Null elements will be placed at the end of
    the returned array. May throw if E is and ARRAY or ROW type and input values contain
    nested nulls. Throws if deciding the order of elements would require comparing nested
    null values. ::

        SELECT array_sort_desc(ARRAY ['cat', 'leopard', 'mouse'], x -> length(x)); -- ['leopard', 'mouse', 'cat']

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

.. function:: concat(array1, array2, ..., arrayN) -> array

    Concatenates the arrays ``array1``, ``array2``, ..., ``arrayN``. This function provides the same functionality as the SQL-standard concatenation operator (``||``).

.. function:: contains(x, element) -> boolean

    Returns true if the array ``x`` contains the ``element``.
    When 'element' is of complex type, throws if 'x' or 'element' contains nested nulls
    and these need to be compared to produce a result.
    For REAL and DOUBLE, NANs (Not-a-Number) are considered equal. ::

        SELECT contains(ARRAY [2.1, 1.1, nan()], nan()); -- true.
        SELECT contains(ARRAY[ARRAY[1, 3]], ARRAY[2, null]); -- false.
        SELECT contains(ARRAY[ARRAY[2, 3]], ARRAY[2, null]); -- failed: contains does not support arrays with elements that are null or contain null
        SELECT contains(ARRAY[ARRAY[2, null]], ARRAY[2, 1]); -- failed: contains does not support arrays with elements that are null or contain null

.. function:: element_at(array(E), index) -> E

    Returns element of ``array`` at given ``index``.
    If ``index`` > 0, this function provides the same functionality as the SQL-standard subscript operator (``[]``).
    If ``index`` < 0, ``element_at`` accesses elements from the last to the first.

.. function:: filter(array(T), function(T,boolean)) -> array(T)

    Constructs an array from those elements of ``array`` for which ``function`` returns true::

        SELECT filter(ARRAY [], x -> true); -- []
        SELECT filter(ARRAY [5, -6, NULL, 7], x -> x > 0); -- [5, 7]
        SELECT filter(ARRAY [5, NULL, 7, NULL], x -> x IS NOT NULL); -- [5, 7]

.. function:: find_first(array(T), function(T,boolean)) -> T

    Returns the first element of ``array`` that matches the predicate.
    Returns ``NULL`` if no element matches the predicate.
    Throws if the first matching element is NULL to avoid ambiguous results
    for no-match and first-match-is-null cases.

.. function:: find_first(array(T), index, function(T,boolean)) -> E
    :noindex:

    Returns the first element of ``array`` that matches the predicate.
    Returns ``NULL`` if no element matches the predicate.
    Throws if the first matching element is NULL to avoid ambiguous results
    for no-match and first-match-is-null cases.
    If ``index`` > 0, the search for element starts at position ``index``
    until the end of the array.
    If ``index`` < 0, the search for element starts at position ``abs(index)``
    counting from the end of the array, until the start of the array. ::

        SELECT find_first(ARRAY[3, 4, 5, 6], 2, x -> x > 0); -- 4
        SELECT find_first(ARRAY[3, 4, 5, 6], -2, x -> x > 0); -- 5
        SELECT find_first(ARRAY[3, 4, 5, 6], 2, x -> x < 4); -- NULL
        SELECT find_first(ARRAY[3, 4, 5, 6], -2, x -> x > 5); -- NULL

.. function:: find_first_index(array(T), function(T,boolean)) -> BIGINT

    Returns the 1-based index of the first element of ``array`` that matches the predicate.
    Returns ``NULL`` if no such element exists.

.. function:: find_first_index(array(T), index, function(T,boolean)) -> BIGINT
    :noindex:

    Returns the 1-based index of the first element of ``array`` that matches the predicate.
    Returns ``NULL`` if no such element exists.
    If ``index`` > 0, the search for element starts at position ``index`` until the end of the array.
    If ``index`` < 0, the search for element starts at position ``abs(index)`` counting from
    the end of the array, until the start of the array. ::

        SELECT find_first_index(ARRAY[3, 4, 5, 6], 2, x -> x > 0); -- 2
        SELECT find_first_index(ARRAY[3, 4, 5, 6], -2, x -> x > 0); -- 3
        SELECT find_first_index(ARRAY[3, 4, 5, 6], 2, x -> x < 4); -- NULL
        SELECT find_first_index(ARRAY[3, 4, 5, 6], -2, x -> x > 5); -- NULL

.. function:: flatten(array(array(T))) -> array(T)

    Flattens an ``array(array(T))`` to an ``array(T)`` by concatenating the contained arrays.

.. function:: ngrams(array(T), n) -> array(array(T))

    Returns `n-grams <https://en.wikipedia.org/wiki/N-gram>`_  for the array.
    Throws if n is zero or negative. If n is greater or equal to input array,
    result array contains input array as the only item. ::

        SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 2); -- [['foo', 'bar'], ['bar', 'baz'], ['baz', 'foo']]
        SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 3); -- [['foo', 'bar', 'baz'], ['bar', 'baz', 'foo']]
        SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 4); -- [['foo', 'bar', 'baz', 'foo']]
        SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 5); -- [['foo', 'bar', 'baz', 'foo']]
        SELECT ngrams(ARRAY[1, 2, 3, 4], 2); -- [[1, 2], [2, 3], [3, 4]]
        SELECT ngrams(ARRAY["foo", NULL, "bar"], 2); -- [["foo", NULL], [NULL, "bar"]]

.. function:: reduce(array(T), initialState S, inputFunction(S,T,S), outputFunction(S,R)) -> R

    Returns a single value reduced from ``array``. ``inputFunction`` will
    be invoked for each element in ``array`` in order. In addition to taking
    the element, ``inputFunction`` takes the current state, initially
    ``initialState``, and returns the new state. ``outputFunction`` will be
    invoked to turn the final state into the result value. It may be the
    identity function (``i -> i``).

    Throws if array has more than 10,000 elements. ::

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

.. function:: repeat(element, count) -> array(E)

    Repeat ``element`` for ``count`` times. ``count`` cannot be negative and must be less than or equal to 10000.

.. function:: reverse(array(E)) -> array(E)

    Returns an array which has the reversed order of the input array.

.. function:: shuffle(array(E)) -> array(E)

    Generate a random permutation of the given ``array`` ::

        SELECT shuffle(ARRAY [1, 2, 3]); -- [3, 1, 2] or any other random permutation
        SELECT shuffle(ARRAY [0, 0, 0]); -- [0, 0, 0]
        SELECT shuffle(ARRAY [1, NULL, 1, NULL, 2]); -- [2, NULL, NULL, NULL, 1] or any other random permutation

.. function:: slice(array(E), start, length) -> array(E)

    Returns a subarray starting from index ``start``(or starting from the end
    if ``start`` is negative) with a length of ``length``.

.. function:: sequence(start, stop) -> array

    Generate a sequence of integers from start to stop, incrementing by 1 if start is less than or equal to stop,
    otherwise -1.

.. function:: sequence(start, stop, step) -> array
   :noindex:

    Generate a sequence of integers from start to stop, incrementing by step.

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

.. function:: trim_array(x, n) -> array

    Remove n elements from the end of ``array``::

        SELECT trim_array(ARRAY[1, 2, 3, 4], 1); -- [1, 2, 3]
        SELECT trim_array(ARRAY[1, 2, 3, 4], 2); -- [1, 2]
        SELECT trim_array(ARRAY[1, 2, 3, 4], 4); -- []

.. function:: remove_nulls(x) -> array

    Remove null values from an array ``array`` ::

        SELECT remove_nulls(ARRAY[1, NULL, 3, NULL]); -- [1, 3]
        SELECT remove_nulls(ARRAY[true, false, NULL]); -- [true, false]
        SELECT remove_nulls(ARRAY[ARRAY[1, 2], NULL, ARRAY[1, NULL, 3]]); -- [[1, 2], [1, null, 3]]

.. function:: zip(array(T), array(U),..) -> array(row(T,U, ...))

    Returns the merge of the given arrays, element-wise into a single array of rows.
    The M-th element of the N-th argument will be the N-th field of the M-th output element.
    If the arguments have an uneven length, missing values are filled with ``NULL`` ::

        SELECT zip(ARRAY[1, 2], ARRAY['1b', null, '3b']); -- [ROW(1, '1b'), ROW(2, null), ROW(null, '3b')]

.. function:: zip_with(array(T), array(U), function(T,U,R)) -> array(R)

    Merges the two given arrays, element-wise, into a single array using ``function``.
    If one array is shorter, nulls are appended at the end to match the length of the
    longer array, before applying ``function`` ::

        SELECT zip_with(ARRAY[1, 3, 5], ARRAY['a', 'b', 'c'], (x, y) -> (y, x)); -- [ROW('a', 1), ROW('b', 3), ROW('c', 5)]
        SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4], (x, y) -> x + y); -- [4, 6]
        SELECT zip_with(ARRAY['a', 'b', 'c'], ARRAY['d', 'e', 'f'], (x, y) -> concat(x, y)); -- ['ad', 'be', 'cf']
        SELECT zip_with(ARRAY['a'], ARRAY['d', null, 'f'], (x, y) -> coalesce(x, y)); -- ['a', null, 'f']
