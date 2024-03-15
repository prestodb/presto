===========================
Map Functions
===========================

.. function:: all_keys_match(x(K,V), function(K, boolean)) -> boolean

    Returns whether all keys of a map match the given predicate. Returns true if all the keys match the predicate (a special case is when the map is empty); false if one or more keys don’t match; NULL if the predicate function returns NULL for one or more keys and true for all other keys. ::

        SELECT all_keys_match(map(array['a', 'b', 'c'], array[1, 2, 3]), x -> length(x) = 1); -- true

.. function:: any_keys_match(x(K,V), function(K, boolean)) -> boolean

    Returns whether any keys of a map match the given predicate. Returns true if one or more keys match the predicate; false if none of the keys match (a special case is when the map is empty); NULL if the predicate function returns NULL for one or more keys and false for all other keys. ::

        SELECT any_keys_match(map(array['a', 'b', 'c'], array[1, 2, 3]), x -> x = 'a'); -- true

.. function:: any_values_match(x(K,V), function(V, boolean)) -> boolean

    Returns whether any values of a map matches the given predicate. Returns true if one or more values match the predicate; false if none of the values match (a special case is when the map is empty); NULL if the predicate function returns NULL for one or more values and false for all other values. ::

        SELECT ANY_VALUES_MATCH(map(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]), x -> x = 1); -- true

.. function:: cardinality(x) -> bigint
    :noindex:

    Returns the cardinality (size) of the map ``x``.

.. function:: element_at(map(K,V), key) -> V
    :noindex:

    Returns value for given ``key``, or ``NULL`` if the key is not contained in the map.

.. function:: map() -> map<unknown, unknown>

    Returns an empty map. ::

        SELECT map(); -- {}

.. function:: map(array(K), array(V)) -> map(K,V)
   :noindex:

    Returns a map created using the given key/value arrays. Keys are not allowed to be null or to contain nulls. ::

        SELECT map(ARRAY[1,3], ARRAY[2,4]); -- {1 -> 2, 3 -> 4}

    See also :func:`map_agg` for creating a map as an aggregation.

.. function:: map_concat(map1(K,V), map2(K,V), ..., mapN(K,V)) -> map(K,V)

   Returns the union of all the given maps. If a key is found in multiple given maps,
   that key's value in the resulting map comes from the last one of those maps.

.. function:: map_entries(map(K,V)) -> array(row(K,V))

    Returns an array of all entries in the given map. ::

        SELECT map_entries(MAP(ARRAY[1, 2], ARRAY['x', 'y'])); -- [ROW(1, 'x'), ROW(2, 'y')]

.. function:: map_filter(map(K,V), function(K,V,boolean)) -> map(K,V)

    Constructs a map from those entries of ``map`` for which ``function`` returns true::

        SELECT map_filter(MAP(ARRAY[], ARRAY[]), (k, v) -> true); -- {}
        SELECT map_filter(MAP(ARRAY[10, 20, 30], ARRAY['a', NULL, 'c']), (k, v) -> v IS NOT NULL); -- {10 -> a, 30 -> c}
        SELECT map_filter(MAP(ARRAY['k1', 'k2', 'k3'], ARRAY[20, 3, 15]), (k, v) -> v > 10); -- {k1 -> 20, k3 -> 15}

.. function:: map_from_entries(array(row(K, V))) -> map(K, V)

    Returns a map created from the given array of entries. Keys are not allowed to be null or to contain nulls. ::

        SELECT map_from_entries(ARRAY[(1, 'x'), (2, 'y')]); -- {1 -> 'x', 2 -> 'y'}

.. function:: map_normalize(map(varchar,double)) -> map(varchar,double)

    Returns the map with the same keys but all non-null values scaled proportionally
    so that the sum of values becomes 1. Map entries with null values remain unchanged.

    When total sum of non-null values is zero, null values remain null,
    zero, NaN, Infinity and -Infinity values become NaN,
    positive values become Infinity, negative values become -Infinity.::

        SELECT map_normalize(map(array['a', 'b', 'c'], array[1, 4, 5])); -- {a=0.1, b=0.4, c=0.5}
        SELECT map_normalize(map(array['a', 'b', 'c', 'd'], array[1, null, 4, 5])); -- {a=0.1, b=null, c=0.4, d=0.5}
        SELECT map_normalize(map(array['a', 'b', 'c'], array[1, 0, -1])); -- {a=Infinity, b=NaN, c=-Infinity}


.. function:: map_subset(map(K,V), array(k)) -> map(K,V)

    Constructs a map from those entries of ``map`` for which the key is in the array given::

        SELECT map_subset(MAP(ARRAY[1,2], ARRAY['a','b']), ARRAY[10]); -- {}
        SELECT map_subset(MAP(ARRAY[1,2], ARRAY['a','b']), ARRAY[1]); -- {1->'a'}
        SELECT map_subset(MAP(ARRAY[1,2], ARRAY['a','b']), ARRAY[1,3]); -- {1->'a'}
        SELECT map_subset(MAP(ARRAY[1,2], ARRAY['a','b']), ARRAY[]); -- {}
        SELECT map_subset(MAP(ARRAY[], ARRAY[]), ARRAY[1,2]); -- {}

.. function:: map_top_n(map(K,V), n) -> map(K, V)

    Truncates map items. Keeps only the top N elements by value.
    ``n`` must be a non-negative BIGINT value.::

        SELECT map_top_n(map(ARRAY['a', 'b', 'c'], ARRAY[2, 3, 1]), 2) --- {'b' -> 3, 'a' -> 2}
        SELECT map_top_n(map(ARRAY['a', 'b', 'c'], ARRAY[NULL, 3, NULL]), 2) --- {'b' -> 3, 'a' -> NULL}

.. function:: map_keys(x(K,V)) -> array(K)

    Returns all the keys in the map ``x``.

.. function:: map_values(x(K,V)) -> array(V)

    Returns all the values in the map ``x``.

.. function:: map_zip_with(map(K,V1), map(K,V2), function(K,V1,V2,V3)) -> map(K,V3)

    Merges the two given maps into a single map by applying ``function`` to the pair of values with the same key.
    For keys only presented in one map, NULL will be passed as the value for the missing key. ::

        SELECT map_zip_with(MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), -- {1 -> ad, 2 -> be, 3 -> cf}
                            MAP(ARRAY[1, 2, 3], ARRAY['d', 'e', 'f']),
                            (k, v1, v2) -> concat(v1, v2));
        SELECT map_zip_with(MAP(ARRAY['k1', 'k2'], ARRAY[1, 2]), -- {k1 -> ROW(1, null), k2 -> ROW(2, 4), k3 -> ROW(null, 9)}
                            MAP(ARRAY['k2', 'k3'], ARRAY[4, 9]),
                            (k, v1, v2) -> (v1, v2));
        SELECT map_zip_with(MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 8, 27]), -- {a -> a1, b -> b4, c -> c9}
                            MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]),
                            (k, v1, v2) -> k || CAST(v1/v2 AS VARCHAR));

.. function:: multimap_from_entries(array(row(K,V))) -> map(K,array(V))

    Returns a multimap created from the given array of entries. Each key can be associated with multiple values. ::

        SELECT multimap_from_entries(ARRAY[(1, 'x'), (2, 'y'), (1, 'z')]); -- {1 -> ['x', 'z'], 2 -> ['y']}

.. function:: no_keys_match(x(K,V), function(K, boolean)) -> boolean

    Returns whether no keys of a map match the given predicate. Returns true if none of the keys match the predicate (a special case is when the map is empty); false if one or more keys match; NULL if the predicate function returns NULL for one or more keys and false for all other keys. ::

        SELECT no_keys_match(map(array['a', 'b', 'c'], array[1, 2, 3]), x -> x = 'd'); -- true

.. function:: no_values_match(x(K,V), function(V, boolean)) -> boolean

    Returns whether no values of a map match the given predicate. Returns true if none of the values match the predicate (a special case is when the map is empty); false if one or more values match; NULL if the predicate function returns NULL for one or more values and false for all other values. ::

        SELECT no_values_match(map(array['a', 'b', 'c'], array[1, 2, 3]), x -> x = 'd'); -- true

.. function:: subscript(map(K, V), key) -> V
   :noindex:

    Returns value for given ``key``. Return null if the key is not contained in the map.
    Corresponds to SQL subscript operator [].

    SELECT name_to_age_map['Bob'] AS bob_age;

.. function:: transform_keys(map(K1,V), function(K1,V,K2)) -> map(K2,V)

    Returns a map that applies ``function`` to each entry of ``map`` and transforms the keys::

        SELECT transform_keys(MAP(ARRAY[], ARRAY[]), (k, v) -> k + 1); -- {}
        SELECT transform_keys(MAP(ARRAY [1, 2, 3], ARRAY ['a', 'b', 'c']), (k, v) -> k + 1); -- {2 -> a, 3 -> b, 4 -> c}
        SELECT transform_keys(MAP(ARRAY ['a', 'b', 'c'], ARRAY [1, 2, 3]), (k, v) -> v * v); -- {1 -> 1, 4 -> 2, 9 -> 3}
        SELECT transform_keys(MAP(ARRAY ['a', 'b'], ARRAY [1, 2]), (k, v) -> k || CAST(v as VARCHAR)); -- {a1 -> 1, b2 -> 2}
        SELECT transform_keys(MAP(ARRAY [1, 2], ARRAY [1.0, 1.4]), -- {one -> 1.0, two -> 1.4}
                              (k, v) -> MAP(ARRAY[1, 2], ARRAY['one', 'two'])[k]);

.. function:: transform_values(map(K,V1), function(K,V1,V2)) -> map(K,V2)

    Returns a map that applies ``function`` to each entry of ``map`` and transforms the values::

        SELECT transform_values(MAP(ARRAY[], ARRAY[]), (k, v) -> v + 1); -- {}
        SELECT transform_values(MAP(ARRAY [1, 2, 3], ARRAY [10, 20, 30]), (k, v) -> v + k); -- {1 -> 11, 2 -> 22, 3 -> 33}
        SELECT transform_values(MAP(ARRAY [1, 2, 3], ARRAY ['a', 'b', 'c']), (k, v) -> k * k); -- {1 -> 1, 2 -> 4, 3 -> 9}
        SELECT transform_values(MAP(ARRAY ['a', 'b'], ARRAY [1, 2]), (k, v) -> k || CAST(v as VARCHAR)); -- {a -> a1, b -> b2}
        SELECT transform_values(MAP(ARRAY [1, 2], ARRAY [1.0, 1.4]), -- {1 -> one_1.0, 2 -> two_1.4}
                                (k, v) -> MAP(ARRAY[1, 2], ARRAY['one', 'two'])[k] || '_' || CAST(v AS VARCHAR));
