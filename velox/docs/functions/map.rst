===========================
Map Functions
===========================

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

    See also :func:`map_agg` for creating a map as an aggregation.

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

.. function:: subscript(map(K, V), key) -> V

    Returns value for given ``key``. Throws if the key is not contained in the map.
    Corresponds to SQL subscript operator [].

    SELECT name_to_age_map['Bob'] AS bob_age;
