===========================
Map Functions
===========================

.. spark:function:: element_at(map(K,V), key) -> V

    Returns value for given ``key``, or ``NULL`` if the key is not contained in the map.

.. spark:function:: map() -> map(unknown, unknown)

    Returns an empty map. ::

        SELECT map(); -- {}

.. spark:function:: map(array(K), array(V)) -> map(K,V)

    Returns a map created using the given key/value arrays. Duplicate map key will cause exception. ::

        SELECT map(ARRAY[1,3], ARRAY[2,4]); -- {1 -> 2, 3 -> 4}

.. spark:function:: map_filter(map(K,V), func) -> map(K,V)

    Filters entries in a map using the function. ::

        SELECT map_filter(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v); -- {1 -> 0, 3 -> -1}

.. spark:function:: map_from_arrays(array(K), array(V)) -> map(K,V)

    Creates a map with a pair of the given key/value arrays. All elements in keys should not be null. 
    If key size != value size will throw exception that key and value must have the same length.::

        SELECT map_from_arrays(array(1.0, 3.0), array('2', '4')); -- {1.0 -> 2, 3.0 -> 4}

.. spark:function:: size(map(K,V)) -> bigint

    Returns the size of the input map. Returns null for null input
    if :doc:`spark.legacy-size-of-null <../../configs>` is set to false. 
    Otherwise, returns -1 for null input.
