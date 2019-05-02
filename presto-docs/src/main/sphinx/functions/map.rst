===========================
Map Functions and Operators
===========================

Subscript Operator: []
----------------------

The ``[]`` operator is used to retrieve the value corresponding to a given key from a map::

    SELECT name_to_age_map['Bob'] AS bob_age;

Map Functions
-------------

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

    Returns a map created using the given key/value arrays. ::

        SELECT map(ARRAY[1,3], ARRAY[2,4]); -- {1 -> 2, 3 -> 4}

    See also :func:`map_agg` and :func:`multimap_agg` for creating a map as an aggregation.

.. function:: map_from_entries(array(row(K,V))) -> map(K,V)

    Returns a map created from the given array of entries. ::

        SELECT map_from_entries(ARRAY[(1, 'x'), (2, 'y')]); -- {1 -> 'x', 2 -> 'y'}

.. function:: multimap_from_entries(array(row(K,V))) -> map(K,array(V))

    Returns a multimap created from the given array of entries. Each key can be associated with multiple values. ::

        SELECT multimap_from_entries(ARRAY[(1, 'x'), (2, 'y'), (1, 'z')]); -- {1 -> ['x', 'z'], 2 -> ['y']}

.. function:: map_entries(map(K,V)) -> array(row(K,V))

    Returns an array of all entries in the given map. ::

        SELECT map_entries(MAP(ARRAY[1, 2], ARRAY['x', 'y'])); -- [ROW(1, 'x'), ROW(2, 'y')]

.. function:: map_concat(map1(K,V), map2(K,V), ..., mapN(K,V)) -> map(K,V)

   Returns the union of all the given maps. If a key is found in multiple given maps,
   that key's value in the resulting map comes from the last one of those maps.

.. function:: map_filter(map(K,V), function(K,V,boolean)) -> map(K,V)

    Constructs a map from those entries of ``map`` for which ``function`` returns true::

        SELECT map_filter(MAP(ARRAY[], ARRAY[]), (k, v) -> true); -- {}
        SELECT map_filter(MAP(ARRAY[10, 20, 30], ARRAY['a', NULL, 'c']), (k, v) -> v IS NOT NULL); -- {10 -> a, 30 -> c}
        SELECT map_filter(MAP(ARRAY['k1', 'k2', 'k3'], ARRAY[20, 3, 15]), (k, v) -> v > 10); -- {k1 -> 20, k3 -> 15}

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
