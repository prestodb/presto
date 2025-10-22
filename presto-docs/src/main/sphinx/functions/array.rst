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

.. function:: all_match(array(T), function(T,boolean)) -> boolean

    Returns whether all elements of an array match the given predicate. Returns ``true`` if all the elements
    match the predicate (a special case is when the array is empty); ``false`` if one or more elements don't
    match; ``NULL`` if the predicate function returns ``NULL`` for one or more elements and ``true`` for all
    other elements.

.. function:: any_match(array(T), function(T,boolean)) -> boolean

    Returns whether any elements of an array match the given predicate. Returns ``true`` if one or more
    elements match the predicate; ``false`` if none of the elements matches (a special case is when the
    array is empty); ``NULL`` if the predicate function returns ``NULL`` for one or more elements and ``false``
    for all other elements.

.. function:: array_average(array(double)) -> double

    Returns the average of all non-null elements of the ``array``. If there is no non-null elements, returns
    ``null``.

.. function:: array_cum_sum(array(T)) -> array(T)

    Returns the array whose elements are the cumulative sum of the input array, i.e. result[i] = input[1]+input[2]+...+input[i].
    If there there is null elements in the array, the cumulative sum at and after the element is null. ::

        SELECT array_cum_sum(ARRAY [1, 2, null, 3]) -- array[1, 3, null, null]

.. function:: array_distinct(x) -> array

    Remove duplicate values from the array ``x``.
    This function uses ``IS DISTINCT FROM`` to determine the distinct elements. ::

        SELECT array_distinct(ARRAY [1, 2, null, null, 2]) -- ARRAY[1, 2, null]
        SELECT array_distinct(ARRAY [ROW(1, null), ROW (1, null)] -- ARRAY[ROW(1, null)

.. function:: array_duplicates(array(T)) -> array(bigint/varchar)

    Returns a set of elements that occur more than once in ``array``.
    Throws an exception if any of the elements are rows or arrays that contain nulls. ::

        SELECT array_duplicates(ARRAY[1, 2, null, 1, null, 3]) -- ARRAY[1, null]
        SELECT array_duplicates(ARRAY[ROW(1, null), ROW(1, null)]) -- "map key cannot be null or contain nulls"

.. function:: array_except(x, y) -> array

    Returns an array of elements in ``x`` but not in ``y``, without duplicates.
    This function uses ``IS NOT DISTINCT FROM`` to determine which elements are the same. ::

        SELECT array_except(ARRAY[1, 3, 3, 2, null], ARRAY[1,2, 2, 4]) -- ARRAY[3, null]

.. function:: array_frequency(array(E)) -> map(E, int)

    Returns a map: keys are the unique elements in the ``array``, values are how many times the key appears.
    Ignores null elements. Empty array returns empty map.

.. function:: array_has_duplicates(array(T)) -> boolean

    Returns a boolean: whether ``array`` has any elements that occur more than once.
    Throws an exception if any of the elements are rows or arrays that contain nulls. ::

        SELECT array_has_duplicates(ARRAY[1, 2, null, 1, null, 3]) -- true
        SELECT array_has_duplicates(ARRAY[ROW(1, null), ROW(1, null)]) -- "map key cannot be null or contain nulls"

.. function:: array_intersect(x, y) -> array

    Returns an array of the elements in the intersection of ``x`` and ``y``, without duplicates.
    This function uses ``IS NOT DISTINCT FROM`` to determine which elements are the same. ::

        SELECT array_intersect(ARRAY[1, 2, 3, 2, null], ARRAY[1,2, 2, 4, null]) -- ARRAY[1, 2, null]

.. function:: array_intersect(array(array(E))) -> array(E)

    Returns an array of the elements in the intersection of all arrays in the given array, without duplicates.
    This function uses ``IS NOT DISTINCT FROM`` to determine which elements are the same. ::

        SELECT array_intersect(ARRAY[ARRAY[1, 2, 3, 2, null], ARRAY[1,2,2, 4, null], ARRAY [1, 2, 3, 4 null]])  -- ARRAY[1, 2, null]

.. function:: array_join(x, delimiter, null_replacement) -> varchar

    Concatenates the elements of the given array using the delimiter and an optional string to replace nulls.

.. function:: array_least_frequent(array(T)) -> array(T)

    Returns the least frequent non-null element of an array. If there are multiple elements with the same frequency, the function returns the smallest element.
    If the array has more than one element and any elements are ``ROWS`` with null fields or ``ARRAYS`` with null elements, an exception is returned. ::

        SELECT array_least_frequent(ARRAY[1, 0 , 5])  -- ARRAY[0]
        select array_least_frequent(ARRAY[1, null, 1]) -- ARRAY[1]
        select array_least_frequent(ARRAY[ROW(1,null), ROW(1, null)]) -- "map key cannot be null or contain nulls"

.. function:: array_least_frequent(array(T), n) -> array(T)

    Returns ``n`` least frequent non-null elements of an array. The elements are ordered in increasing order of their frequencies.
    If two elements have the same frequency, smaller elements will appear first.
    If the array has more than one element and any elements are ``ROWS`` with null fields or ``ARRAYS`` with null elements, an exception is returned. ::

        SELECT array_least_frequent(ARRAY[3, 2, 2, 6, 6, 1, 1], 3) -- ARRAY[3, 1, 2]
        select array_least_frequent(ARRAY[1, null, 1], 2) -- ARRAY[1]
        select array_least_frequent(ARRAY[ROW(1,null), ROW(1, null)], 2) -- "map key cannot be null or contain nulls"

.. function:: array_max(x) -> x

    Returns the maximum value of input array.

.. function:: array_min(x) -> x

    Returns the minimum value of input array.

.. function:: array_max_by(array(T), function(T, U)) -> T

    Applies the provided function to each element, and returns the element that gives the maximum value.
    ``U`` can be any orderable type. ::

        SELECT array_max_by(ARRAY ['a', 'bbb', 'cc'], x -> LENGTH(x)) -- 'bbb'

.. function:: array_min_by(array(T), function(T, U)) -> T

    Applies the provided function to each element, and returns the element that gives the minimum value.
    ``U`` can be any orderable type. ::

        SELECT array_min_by(ARRAY ['a', 'bbb', 'cc'], x -> LENGTH(x)) -- 'a'

.. function:: array_normalize(x, p) -> array

   Normalizes array ``x`` by dividing each element by the p-norm of the array.
   It is equivalent to ``TRANSFORM(array, v -> v / REDUCE(array, 0, (a, v) -> a + POW(ABS(v), p), a -> POW(a, 1 / p))``,
   but the reduce part is only executed once.
   Returns null if the array is null or there are null array elements.

.. function:: array_position(x, element) -> bigint

    Returns the position of the first occurrence of the ``element`` in array ``x`` (or 0 if not found).

.. function:: array_position(x, element, instance) -> bigint

    If ``instance > 0``, returns the position of the `instance`-th occurrence of the ``element`` in array ``x``. If
    ``instance < 0``, returns the position of the ``instance``-to-last occurrence of the ``element`` in array ``x``.
    If no matching element instance is found, ``0`` is returned.

.. function:: array_remove(x, element) -> array

    Remove all elements that equal ``element`` from array ``x``.

.. function:: array_sort(x) -> array

    Sorts and returns the array ``x``. The elements of ``x`` must be orderable.
    Null elements are placed at the end of the returned array.

.. function:: array_sort(array(T), function(T,T,int)) -> array(T)

    Sorts and returns the ``array`` based on the given comparator ``function``. The comparator will take
    two nullable arguments representing two nullable elements of the ``array``. It returns -1, 0, or 1
    as the first nullable element is less than, equal to, or greater than the second nullable element.
    If the comparator function returns other values (including ``NULL``), the query will fail and raise an error ::

        SELECT array_sort(ARRAY [3, 2, 5, 1, 2], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))); -- [5, 3, 2, 2, 1]
        SELECT array_sort(ARRAY ['bc', 'ab', 'dc'], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))); -- ['dc', 'bc', 'ab']
        SELECT array_sort(ARRAY [3, 2, null, 5, null, 1, 2], -- sort null first with descending order
                          (x, y) -> CASE WHEN x IS NULL THEN -1
                                         WHEN y IS NULL THEN 1
                                         WHEN x < y THEN 1
                                         WHEN x = y THEN 0
                                         ELSE -1 END); -- [null, null, 5, 3, 2, 2, 1]
        SELECT array_sort(ARRAY [3, 2, null, 5, null, 1, 2], -- sort null last with descending order
                          (x, y) -> CASE WHEN x IS NULL THEN 1
                                         WHEN y IS NULL THEN -1
                                         WHEN x < y THEN 1
                                         WHEN x = y THEN 0
                                         ELSE -1 END); -- [5, 3, 2, 2, 1, null, null]
        SELECT array_sort(ARRAY ['a', 'abcd', 'abc'], -- sort by string length
                          (x, y) -> IF(length(x) < length(y),
                                       -1,
                                       IF(length(x) = length(y), 0, 1))); -- ['a', 'abc', 'abcd']
        SELECT array_sort(ARRAY [ARRAY[2, 3, 1], ARRAY[4, 2, 1, 4], ARRAY[1, 2]], -- sort by array length
                          (x, y) -> IF(cardinality(x) < cardinality(y),
                                       -1,
                                       IF(cardinality(x) = cardinality(y), 0, 1))); -- [[1, 2], [2, 3, 1], [4, 2, 1, 4]]

.. function:: array_sort(array(T), function(T,U)) -> array(T)

    Sorts and returns the ``array`` using a lambda function to extract sorting keys. The function is applied
    to each element of the array to produce a key, and the array is sorted based on these keys in ascending order.
    Null array elements and null keys are placed at the end. ::

        SELECT array_sort(ARRAY['pear', 'apple', 'banana', 'kiwi'], x -> length(x)); -- ['pear', 'kiwi', 'apple', 'banana']
        SELECT array_sort(ARRAY[5, 20, 3, 9, 100], x -> x); -- [3, 5, 9, 20, 100]
        SELECT array_sort(ARRAY['apple', NULL, 'banana', NULL], x -> length(x)); -- ['apple', 'banana', NULL, NULL]
        SELECT array_sort(ARRAY[CAST(0.0 AS DOUBLE), CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE)], x -> x); -- [-Infinity, 0.0, Infinity, NaN]
        SELECT array_sort(ARRAY[ROW('a', 3), ROW('b', 1), ROW('c', 2)], x -> x[2]); -- [ROW('b', 1), ROW('c', 2), ROW('a', 3)]

.. function:: array_sort_desc(x) -> array

    Returns the ``array`` sorted in the descending order. Elements of the ``array`` must be orderable.
    Null elements are placed at the end of the returned array. ::

        SELECT array_sort_desc(ARRAY [100, 1, 10, 50]); -- [100, 50, 10, 1]
        SELECT array_sort_desc(ARRAY [null, 100, null, 1, 10, 50]); -- [100, 50, 10, 1, null, null]
        SELECT array_sort_desc(ARRAY [ARRAY ["a", null], null, ARRAY ["a"]); -- [["a", null], ["a"], null]

.. function:: array_sort_desc(array(T), function(T,U)) -> array(T)

    Sorts and returns the ``array`` in descending order using a lambda function to extract sorting keys.
    The function is applied to each element of the array to produce a key, and the array is sorted based
    on these keys in descending order. Null array elements and null keys are placed at the end. ::

        SELECT array_sort_desc(ARRAY['pear', 'apple', 'banana', 'kiwi'], x -> length(x)); -- ['banana', 'apple', 'pear', 'kiwi']
        SELECT array_sort_desc(ARRAY[5, 20, 3, 9, 100], x -> x); -- [100, 20, 9, 5, 3]
        SELECT array_sort_desc(ARRAY['apple', NULL, 'banana', NULL], x -> length(x)); -- ['banana', 'apple', NULL, NULL]
        SELECT array_sort_desc(ARRAY[CAST(0.0 AS DOUBLE), CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE)], x -> x); -- [NaN, Infinity, 0.0, -Infinity]
        SELECT array_sort_desc(ARRAY[ROW('a', 3), ROW('b', 1), ROW('c', 2)], x -> x[2]); -- [ROW('a', 3), ROW('c', 2), ROW('b', 1)]

.. function:: array_split_into_chunks(array(T), int) -> array(array(T))

    Returns an ``array`` of arrays splitting the input ``array`` into chunks of given length.
    The last chunk will be shorter than the chunk length if the array's length is not an integer multiple of
    the chunk length. Ignores null inputs, but not elements.

        SELECT array_split_into_chunks(ARRAY [1, 2, 3, 4], 3); -- [[1, 2, 3], [4]]
        SELECT array_split_into_chunks(null, null); -- null
        SELECT array_split_into_chunks(array[1, 2, 3, cast(null as int)], 2]); -- [[1, 2], [3, null]]

.. function:: array_sum(array(T)) -> bigint/double

    Returns the sum of all non-null elements of the ``array``. If there is no non-null elements, returns ``0``.
    The behavior is similar to aggregation function :func:`!sum`.

    ``T`` must be coercible to ``double``.
    Returns ``bigint`` if T is coercible to ``bigint``. Otherwise, returns ``double``.

.. function:: array_top_n(array(T), int) -> array(T)

    Returns an array of the top ``n`` elements from a given ``array``, sorted according to its natural descending order.
    If ``n`` is larger than the size of the given ``array``, the returned list will be the same size as the input instead of ``n``. ::

        SELECT array_top_n(ARRAY [1, 100, 2, 5, 3], 3); -- [100, 5, 3]
        SELECT array_top_n(ARRAY [1, 100], 5); -- [100, 1]
        SELECT array_top_n(ARRAY ['a', 'zzz', 'zz', 'b', 'g', 'f'], 3); -- ['zzz', 'zz', 'g']

.. function:: arrays_overlap(x, y) -> boolean

    Tests if arrays ``x`` and ``y`` have any non-null elements in common.
    Returns null if there are no non-null elements in common but either array contains null.
    Throws a ``NOT_SUPPORTED`` exception on elements of ``ROW`` or ``ARRAY`` type that contain null values. ::

        SELECT arrays_overlap(ARRAY [1, 2, null], ARRAY [2, 3, null]) -- true
        SELECT arrays_overlap(ARRAY [1, 2], ARRAY [3, 4]) -- false
        SELECT arrays_overlap(ARRAY [1, null], ARRAY[2]) -- null
        SELECT arrays_overlap(ARRAY[ROW(1, null)], ARRAY[1, 2]) -- "ROW comparison not supported for fields with null elements"

.. function:: array_union(x, y) -> array

    Returns an array of the elements in the union of ``x`` and ``y``, without duplicates.
    This function uses ``IS NOT DISTINCT FROM`` to determine which elements are the same. ::

        SELECT array_union(ARRAY[1, 2, 3, 2, null], ARRAY[1,2, 2, 4, null]) -- ARRAY[1, 2, 3, 4 null]

.. function:: cardinality(x) -> bigint

    Returns the cardinality (size) of the array ``x``.

.. function:: concat(array1, array2, ..., arrayN) -> array
    :noindex:

    Concatenates the arrays ``array1``, ``array2``, ``...``, ``arrayN``.
    This function provides the same functionality as the SQL-standard concatenation operator (``||``).

.. function:: combinations(array(T), n) -> array(array(T))

    Returns n-element combinations of the input array.
    If the input array has no duplicates, ``combinations`` returns n-element subsets.
    Order of subgroup is deterministic but unspecified. Order of elements within
    a subgroup are deterministic but unspecified. ``n`` must not be greater than 5,
    and the total size of subgroups generated must be smaller than 100000::

        SELECT combinations(ARRAY['foo', 'bar', 'boo'],2); --[['foo', 'bar'], ['foo', 'boo']['bar', 'boo']]
        SELECT combinations(ARRAY[1,2,3,4,5],3); --[[1,2,3], [1,2,4], [1,3,4], [2,3,4]]
        SELECT combinations(ARRAY[1,2,2],2); --[[1,2],[1,2],[2,2]]

.. function:: contains(x, element) -> boolean

    Returns true if the array ``x`` contains the ``element``.

.. function:: element_at(array(E), index) -> E

    Returns element of ``array`` at given ``index``.
    If ``index`` > 0, this function provides the same functionality as the SQL-standard subscript operator (``[]``), except that it returns ``NULL`` when ``index`` is out of bounds.
    If ``index`` < 0, ``element_at`` accesses elements from the last to the first.

.. function:: filter(array(T), function(T,boolean)) -> array(T)

    Constructs an array from those elements of ``array`` for which ``function`` returns true::

        SELECT filter(ARRAY [], x -> true); -- []
        SELECT filter(ARRAY [5, -6, NULL, 7], x -> x > 0); -- [5, 7]
        SELECT filter(ARRAY [5, NULL, 7, NULL], x -> x IS NOT NULL); -- [5, 7]

.. function:: flatten(x) -> array

    Flattens an ``array(array(T))`` to an ``array(T)`` by concatenating the contained arrays.

.. function:: find_first(array(E), function(T,boolean)) -> E

    Returns the first element of ``array`` which returns true for ``function(T,boolean)``, throws exception if the returned element is NULL.
    Returns ``NULL`` if no such element exists.

.. function:: find_first(array(E), index, function(T,boolean)) -> E

    Returns the first element of ``array`` which returns true for ``function(T,boolean)``, throws exception if the returned element is NULL.
    Returns ``NULL`` if no such element exists.
    If ``index`` > 0, the search for element starts at position ``index`` until the end of array.
    If ``index`` < 0, the search for element starts at position ``abs(index)`` counting from last, until the start of array. ::

        SELECT find_first(ARRAY[3, 4, 5, 6], 2, x -> x > 0); -- 4
        SELECT find_first(ARRAY[3, 4, 5, 6], -2, x -> x > 0); -- 5
        SELECT find_first(ARRAY[3, 4, 5, 6], 2, x -> x < 4); -- NULL
        SELECT find_first(ARRAY[3, 4, 5, 6], -2, x -> x > 5); -- NULL

.. function:: find_first_index(array(E), function(T,boolean)) -> BIGINT

    Returns the index of the first element of ``array`` which returns true for ``function(T,boolean)``.
    Returns ``NULL`` if no such element exists.

.. function:: find_first_index(array(E), index, function(T,boolean)) -> BIGINT

    Returns the index of the first element of ``array`` which returns true for ``function(T,boolean)``.
    Returns ``NULL`` if no such element exists.
    If ``index`` > 0, the search for element starts at position ``index`` until the end of array.
    If ``index`` < 0, the search for element starts at position ``abs(index)`` counting from last, until the start of array. ::

        SELECT find_first(ARRAY[3, 4, 5, 6], 2, x -> x > 0); -- 2
        SELECT find_first(ARRAY[3, 4, 5, 6], -2, x -> x > 0); -- 3
        SELECT find_first(ARRAY[3, 4, 5, 6], 2, x -> x < 4); -- NULL
        SELECT find_first(ARRAY[3, 4, 5, 6], -2, x -> x > 5); -- NULL

.. function:: ngrams(array(T), n) -> array(array(T))

    Returns ``n``-grams for the ``array``::

        SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 2); -- [['foo', 'bar'], ['bar', 'baz'], ['baz', 'foo']]
        SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 3); -- [['foo', 'bar', 'baz'], ['bar', 'baz', 'foo']]
        SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 4); -- [['foo', 'bar', 'baz', 'foo']]
        SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 5); -- [['foo', 'bar', 'baz', 'foo']]
        SELECT ngrams(ARRAY[1, 2, 3, 4], 2); -- [[1, 2], [2, 3], [3, 4]]

.. function:: none_match(array(T), function(T,boolean)) -> boolean

    Returns whether no elements of an array match the given predicate. Returns ``true`` if none of the elements
    matches the predicate (a special case is when the array is empty); ``false`` if one or more elements match;
    ``NULL`` if the predicate function returns ``NULL`` for one or more elements and ``false`` for all other elements.

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

.. function:: remove_nulls(array(T)) -> array

    Remove all null elements in the array.

.. function:: repeat(element, count) -> array

    Repeat ``element`` for ``count`` times.

.. function:: reverse(x) -> array
    :noindex:

    Returns an array which has the reversed order of array ``x``.

.. function:: sequence(start, stop) -> array(bigint)

    Generate a sequence of integers from ``start`` to ``stop``, incrementing
    by ``1`` if ``start`` is less than or equal to ``stop``, otherwise ``-1``.

.. function:: sequence(start, stop, step) -> array(bigint)

    Generate a sequence of integers from ``start`` to ``stop``, incrementing by ``step``.

.. function:: sequence(start, stop) -> array(date)

    Generate a sequence of dates from ``start`` date to ``stop`` date, incrementing
    by ``1`` day if ``start`` date is less than or equal to ``stop`` date, otherwise ``-1`` day.

.. function:: sequence(start, stop, step) -> array(date)

    Generate a sequence of dates from ``start`` to ``stop``, incrementing by ``step``.
    The type of ``step`` can be either ``INTERVAL DAY TO SECOND`` or ``INTERVAL YEAR TO MONTH``.

.. function:: sequence(start, stop, step) -> array(timestamp)

    Generate a sequence of timestamps from ``start`` to ``stop``, incrementing by ``step``.
    The type of ``step`` can be either ``INTERVAL DAY TO SECOND`` or ``INTERVAL YEAR TO MONTH``.

.. function:: shuffle(x) -> array

    Generate a random permutation of the given array ``x``.

.. function:: slice(x, start, length) -> array

    Subsets array ``x`` starting from index ``start`` (or starting from the end
    if ``start`` is negative) with a length of ``length``.

.. function:: trim_array(x, n) -> array

    Remove ``n`` elements from the end of array::

        SELECT trim_array(ARRAY[1, 2, 3, 4], 1);
        -- [1, 2, 3]

        SELECT trim_array(ARRAY[1, 2, 3, 4], 2);
        -- [1, 2]

.. function:: transform(array(T), function(T,U)) -> array(U)

    Returns an array that is the result of applying ``function`` to each element of ``array``::

        SELECT transform(ARRAY [], x -> x + 1); -- []
        SELECT transform(ARRAY [5, 6], x -> x + 1); -- [6, 7]
        SELECT transform(ARRAY [5, NULL, 6], x -> COALESCE(x, 0) + 1); -- [6, 1, 7]
        SELECT transform(ARRAY ['x', 'abc', 'z'], x -> x || '0'); -- ['x0', 'abc0', 'z0']
        SELECT transform(ARRAY [ARRAY [1, NULL, 2], ARRAY[3, NULL]], a -> filter(a, x -> x IS NOT NULL)); -- [[1, 2], [3]]

.. function:: zip(array1, array2[, ...]) -> array(row)

    Merges the given arrays, element-wise, into a single array of rows. The M-th element of
    the N-th argument will be the N-th field of the M-th output element.
    If the arguments have an uneven length, missing values are filled with ``NULL``. ::

        SELECT zip(ARRAY[1, 2], ARRAY['1b', null, '3b']); -- [ROW(1, '1b'), ROW(2, null), ROW(null, '3b')]

.. function:: zip_with(array(T), array(U), function(T,U,R)) -> array(R)

    Merges the two given arrays, element-wise, into a single array using ``function``.
    If one array is shorter, nulls are appended at the end to match the length of the longer array, before applying ``function``::

        SELECT zip_with(ARRAY[1, 3, 5], ARRAY['a', 'b', 'c'], (x, y) -> (y, x)); -- [ROW('a', 1), ROW('b', 3), ROW('c', 5)]
        SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4], (x, y) -> x + y); -- [4, 6]
        SELECT zip_with(ARRAY['a', 'b', 'c'], ARRAY['d', 'e', 'f'], (x, y) -> concat(x, y)); -- ['ad', 'be', 'cf']
        SELECT zip_with(ARRAY['a'], ARRAY['d', null, 'f'], (x, y) -> coalesce(x, y)); -- ['a', null, 'f']
