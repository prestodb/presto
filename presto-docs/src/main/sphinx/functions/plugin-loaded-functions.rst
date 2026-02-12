=======================
Plugin Loaded Functions
=======================

These functions are optional, opt-in functions that can be loaded as needed.
For more details on loading these functions, refer to the
`presto-sql-helpers README. <https://github.com/prestodb/presto/tree/master/presto-sql-helpers>`_

Array Functions
---------------

.. function:: array_intersect(array(array(E))) -> array(E)

    Returns an array of the elements in the intersection of all arrays in the given array, without duplicates.
    This function uses ``IS NOT DISTINCT FROM`` to determine which elements are the same. ::

        SELECT array_intersect(ARRAY[ARRAY[1, 2, 3, 2, null], ARRAY[1, 2, 2, 4, null], ARRAY [1, 2, 3, 4, null]])  -- ARRAY[1, 2, null]

.. function:: array_average(array(double)) -> double

    Returns the average of all non-null elements of the ``array``. If there are no non-null elements, returns
    ``null``.

.. function:: array_split_into_chunks(array(T), int) -> array(array(T))

    Returns an ``array`` of arrays splitting the input ``array`` into chunks of given length.
    The last chunk will be shorter than the chunk length if the array's length is not an integer multiple of
    the chunk length. Ignores null inputs, but not elements. ::

        SELECT array_split_into_chunks(ARRAY [1, 2, 3, 4], 3); -- [[1, 2, 3], [4]]
        SELECT array_split_into_chunks(null, null); -- null
        SELECT array_split_into_chunks(array[1, 2, 3, cast(null as int)], 2); -- [[1, 2], [3, null]]

.. function:: array_frequency(array(E)) -> map(E, int)

    Returns a map: keys are the unique elements in the ``array``, values are how many times the key appears.
    Ignores null elements. Empty array returns empty map.

.. function:: array_duplicates(array(T)) -> array(bigint/varchar)

    Returns a set of elements that occur more than once in ``array``.
    Throws an exception if any of the elements are rows or arrays that contain nulls. ::

        SELECT array_duplicates(ARRAY[1, 2, null, 1, null, 3]) -- ARRAY[1, null]
        SELECT array_duplicates(ARRAY[ROW(1, null), ROW(1, null)]) -- "map key cannot be null or contain nulls"

.. function:: array_has_duplicates(array(T)) -> boolean

    Returns a boolean: whether ``array`` has any elements that occur more than once.
    Throws an exception if any of the elements are rows or arrays that contain nulls. ::

        SELECT array_has_duplicates(ARRAY[1, 2, null, 1, null, 3]) -- true
        SELECT array_has_duplicates(ARRAY[ROW(1, null), ROW(1, null)]) -- "map key cannot be null or contain nulls"

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

.. function:: array_max_by(array(T), function(T, U)) -> T

    Applies the provided function to each element, and returns the element that gives the maximum value.
    ``U`` can be any orderable type. ::

        SELECT array_max_by(ARRAY ['a', 'bbb', 'cc'], x -> LENGTH(x)) -- 'bbb'

.. function:: array_min_by(array(T), function(T, U)) -> T

    Applies the provided function to each element, and returns the element that gives the minimum value.
    ``U`` can be any orderable type. ::

        SELECT array_min_by(ARRAY ['a', 'bbb', 'cc'], x -> LENGTH(x)) -- 'a'

.. function:: array_sort_desc(x) -> array

    Returns the ``array`` sorted in the descending order. Elements of the ``array`` must be orderable.
    Null elements are placed at the end of the returned array. ::

        SELECT array_sort_desc(ARRAY [100, 1, 10, 50]); -- [100, 50, 10, 1]
        SELECT array_sort_desc(ARRAY [null, 100, null, 1, 10, 50]); -- [100, 50, 10, 1, null, null]
        SELECT array_sort_desc(ARRAY [ARRAY ["a", null], null, ARRAY ["a"]]); -- [["a", null], ["a"], null]

.. function:: remove_nulls(array(T)) -> array

    Remove all null elements in the array.

.. function:: array_top_n(array(T), int) -> array(T)

    Returns an array of the top ``n`` elements from a given ``array``, sorted according to its natural descending order.
    If ``n`` is larger than the size of the given ``array``, the returned list will be the same size as the input instead of ``n``. ::

        SELECT array_top_n(ARRAY [1, 100, 2, 5, 3], 3); -- [100, 5, 3]
        SELECT array_top_n(ARRAY [1, 100], 5); -- [100, 1]
        SELECT array_top_n(ARRAY ['a', 'zzz', 'zz', 'b', 'g', 'f'], 3); -- ['zzz', 'zz', 'g']

.. function:: array_top_n(array(T), int, function(T,T,int)) -> array(T)

    Returns an array of the top ``n`` elements from a given ``array`` using the specified comparator ``function``.
    The comparator will take two nullable arguments representing two nullable elements of the ``array``. It returns -1, 0, or 1
    as the first nullable element is less than, equal to, or greater than the second nullable element.
    If the comparator function returns other values (including ``NULL``), the query will fail and raise an error.
    If ``n`` is larger than the size of the given ``array``, the returned list will be the same size as the input instead of ``n``. ::

        SELECT array_top_n(ARRAY [100, 1, 3, -10, 6, -5], 3, (x, y) -> IF(abs(x) < abs(y), -1, IF(abs(x) = abs(y), 0, 1))); -- [100, -10, 6]
        SELECT array_top_n(ARRAY [CAST(ROW(1, 2) AS ROW(x INT, y INT)), CAST(ROW(0, 11) AS ROW(x INT, y INT)), CAST(ROW(5, 10) AS ROW(x INT, y INT))], 2, (a, b) -> IF(a.x*a.y < b.x*b.y, -1, IF(a.x*a.y = b.x*b.y, 0, 1))); -- [ROW(5, 10), ROW(1, 2)]

.. function:: array_transpose(array(array(T))) -> array(array(T))

    Returns a transpose of a 2D array (matrix), where rows become columns and columns become rows.
    Converts ``a[x][y]`` to ``transpose(a)[y][x]``. All rows in the input array must have the same length, otherwise the function will fail with an error.
    Returns an empty array if the input is empty or if all rows are empty. ::

        SELECT array_transpose(ARRAY [ARRAY [1, 2, 3], ARRAY [4, 5, 6]]) -- [[1, 4], [2, 5], [3, 6]]
        SELECT array_transpose(ARRAY [ARRAY ['a', 'b'], ARRAY ['c', 'd'], ARRAY ['e', 'f']]) -- [['a', 'c', 'e'], ['b', 'd', 'f']]
        SELECT array_transpose(ARRAY [ARRAY [1]]) -- [[1]]
        SELECT array_transpose(ARRAY []) -- []

Map Functions
--------------

.. function:: map_normalize(x(varchar,double)) -> map(varchar,double)

    Returns the map with the same keys but all non-null values are scaled proportionally so that the sum of values becomes 1.
    Map entries with null values remain unchanged.

.. function:: map_keys_by_top_n_values(x(K,V), n) -> array(K)

    Returns top ``n`` keys in the map ``x`` by sorting its values in descending order. If two or more keys have equal values, the higher key takes precedence.
    ``n`` must be a non-negative integer.::

        SELECT map_keys_by_top_n_values(map(ARRAY['a', 'b', 'c'], ARRAY[2, 1, 3]), 2) --- ['c', 'a']

.. function:: map_key_exists(x(K, V), k) -> boolean

    Returns whether the given key exists in the map. Returns ``true`` if key is present in the input map, returns ``false`` if not present.::

        SELECT map_key_exists(MAP(ARRAY['x','y'], ARRAY[100,200]), 'x'); -- TRUE

.. function:: map_top_n(x(K,V), n) -> map(K, V)

    Truncates map items. Keeps only the top ``n`` elements by value.  Keys are used to break ties with the max key being chosen. Both keys and values should be orderable.
    ``n`` must be a non-negative integer. ::

        SELECT map_top_n(map(ARRAY['a', 'b', 'c'], ARRAY[2, 3, 1]), 2) --- {'b' -> 3, 'a' -> 2}

.. function:: map_top_n_keys(x(K,V), n) -> array(K)

    Returns top ``n`` keys in the map ``x`` by sorting its keys in descending order.
    ``n`` must be a non-negative integer.

    For bottom ``n`` keys, use the function with lambda operator to perform custom sorting. ::

        SELECT map_top_n_keys(map(ARRAY['a', 'b', 'c'], ARRAY[3, 2, 1]), 2) --- ['c', 'b']

.. function:: map_top_n_keys(x(K,V), n, function(K,K,int)) -> array(K)

    Returns top ``n`` keys in the map ``x`` by sorting its keys using the given comparator ``function``. The comparator takes
    two non-nullable arguments representing two keys of the ``map``. It returns -1, 0, or 1
    as the first key is less than, equal to, or greater than the second key.
    If the comparator function returns other values (including ``NULL``), the query will fail and raise an error. ::

        SELECT map_top_n_keys(map(ARRAY['a', 'b', 'c'], ARRAY[3, 2, 1]), 2, (x, y) -> IF(x < y, -1, IF(x = y, 0, 1))) --- ['c', 'b']

.. function:: map_top_n_values(x(K,V), n) -> array(V)

    Returns top ``n`` values in the map ``x`` by sorting its values in descending order.
    ``n`` must be a non-negative integer. ::

        SELECT map_top_n_values(map(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]), 2) --- [3, 2]

.. function:: map_top_n_values(x(K,V), n, function(V,V,int)) -> array(V)

    Returns top n values in the map ``x`` based on the given comparator ``function``. The comparator will take
    two nullable arguments representing two values of the ``map``. It returns -1, 0, or 1
    as the first value is less than, equal to, or greater than the second value.
    If the comparator function returns other values (including ``NULL``), the query will fail and raise an error. ::

        SELECT map_top_n_values(map(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]), 2, (x, y) -> IF(x < y, -1, IF(x = y, 0, 1))) --- [3, 2]

.. function:: map_remove_null_values(x(K,V)) -> map(K, V)

    Removes all the entries where the value is null from the map ``x``.

.. function:: all_keys_match(x(K,V), function(K, boolean)) -> boolean

    Returns whether all keys of a map match the given predicate. Returns true if all the keys match the predicate (a special case is when the map is empty); false if one or more keys don’t match; NULL if the predicate function returns NULL for one or more keys and true for all other keys. ::

        SELECT all_keys_match(map(array['a', 'b', 'c'], array[1, 2, 3]), x -> length(x) = 1); -- true

.. function:: any_keys_match(x(K,V), function(K, boolean)) -> boolean

    Returns whether any keys of a map match the given predicate. Returns true if one or more keys match the predicate; false if none of the keys match (a special case is when the map is empty); NULL if the predicate function returns NULL for one or more keys and false for all other keys. ::

        SELECT any_keys_match(map(array['a', 'b', 'c'], array[1, 2, 3]), x -> x = 'a'); -- true

.. function:: any_values_match(x(K,V), function(V, boolean)) -> boolean

    Returns whether any values of a map matches the given predicate. Returns true if one or more values match the predicate; false if none of the values match (a special case is when the map is empty); NULL if the predicate function returns NULL for one or more values and false for all other values. ::

        SELECT ANY_VALUES_MATCH(map(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]), x -> x = 1); -- true

.. function:: no_keys_match(x(K,V), function(K, boolean)) -> boolean

    Returns whether no keys of a map match the given predicate. Returns true if none of the keys match the predicate (a special case is when the map is empty); false if one or more keys match; NULL if the predicate function returns NULL for one or more keys and false for all other keys. ::

        SELECT no_keys_match(map(array['a', 'b', 'c'], array[1, 2, 3]), x -> x = 'd'); -- true

.. function:: no_values_match(x(K,V), function(V, boolean)) -> boolean

    Returns whether no values of a map match the given predicate. Returns true if none of the values match the predicate (a special case is when the map is empty); false if one or more values match; NULL if the predicate function returns NULL for one or more values and false for all other values. ::

        SELECT no_values_match(map(array['a', 'b', 'c'], array[1, 2, 3]), x -> x = 'd'); -- true

.. function:: map_int_keys_to_array(map(int,V)) -> array(V)

    Returns an ``array`` of values from the ``map`` with value at indexed by the original keys from ``map``. ::

        SELECT MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[3, 5, 6, 9], ARRAY['a', 'b', 'c', 'd'])) -> ARRAY[null, null, 'a', null, 'b', 'c', null, null, 'd']
        SELECT MAP_INT_KEYS_TO_ARRAY(MAP(ARRAY[3, 5, 6, 9], ARRAY['a', null, 'c', 'd'])) -> ARRAY[null, null, 'a', null, null, 'c', 'd']


.. function:: array_to_map_int_keys(array(v)) -> map(int, v)

    Returns an ``map`` with indices of all non-null values from the ``array`` as keys and element at the specified index as the value. ::

        SELECT ARRAY_TO_MAP_INT_KEYS(CAST(ARRAY[3, 5, 6, 9] AS ARRAY<INT>)) -> MAP(ARRAY[1, 2, 3,4], ARRAY[3, 5, 6, 9])
        SELECT ARRAY_TO_MAP_INT_KEYS(CAST(ARRAY[3, 5, null, 6, 9] AS ARRAY<INT>)) -> MAP(ARRAY[1, 2, 4, 5], ARRAY[3, 5, 6, 9])
        SELECT ARRAY_TO_MAP_INT_KEYS(CAST(ARRAY[3, 5, null, 6, 9, null, null, 1] AS ARRAY<INT>)) -> MAP(ARRAY[1, 2, 4, 5, 8], ARRAY[3, 5, 6, 9, 1])

String Functions
----------------

.. function:: replace_first(string, search, replace) -> varchar

    Replaces the first instance of ``search`` with ``replace`` in ``string``.

    If ``search`` is an empty string, it inserts ``replace`` at the beginning of the ``string``.

.. function:: trail(string, N) -> varchar

    Returns the last N characters of the input string.

.. function:: key_sampling_percent(varchar) -> double

    Generates a double value between 0.0 and 1.0 based on the hash of the given ``varchar``.
    This function is useful for deterministic sampling of data.

