===========================
Map Functions
===========================

.. spark:function:: element_at(map(K,V), key) -> V

    Returns value for given ``key``, or ``NULL`` if the key is not contained in the map.

.. spark:function:: map(K, V, K, V, ...) -> map(K,V)

    Returns a map created using the given key/value pairs. Keys are not allowed to be null. ::

        SELECT map(1, 2, 3, 4); -- {1 -> 2, 3 -> 4}

        SELECT map(array(1, 2), array(3, 4)); -- {[1, 2] -> [3, 4]}

.. spark:function:: map_entries(map(K,V)) -> array(row(K,V))

    Returns an array of all entries in the given map. ::

        SELECT map_entries(MAP(ARRAY[1, 2], ARRAY['x', 'y'])); -- [ROW(1, 'x'), ROW(2, 'y')]

.. spark:function:: map_filter(map(K,V), func) -> map(K,V)

    Filters entries in a map using the function. ::

        SELECT map_filter(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v); -- {1 -> 0, 3 -> -1}

.. spark:function:: map_from_arrays(array(K), array(V)) -> map(K,V)

    Creates a map with a pair of the given key/value arrays. All elements in keys should not be null.
    If key size != value size will throw exception that key and value must have the same length.::

        SELECT map_from_arrays(array(1.0, 3.0), array('2', '4')); -- {1.0 -> 2, 3.0 -> 4}

.. spark:function:: map_keys(x(K,V)) -> array(K)

    Returns all the keys in the map ``x``.

.. spark:function:: map_values(x(K,V)) -> array(V)

    Returns all the values in the map ``x``.

.. spark:function:: map_zip_with(map(K,V1), map(K,V2), function(K,V1,V2,V3)) -> map(K,V3)

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

.. spark:function:: size(map(K,V)) -> bigint
   :noindex:

    Returns the size of the input map. Returns null for null input
    if :doc:`spark.legacy_size_of_null <../../configs>` is set to false.
    Otherwise, returns -1 for null input.
