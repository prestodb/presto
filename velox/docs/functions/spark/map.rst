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

.. spark:function:: size(map(K,V)) -> bigint

    Returns the size of the input map. Returns null for null input
    if :doc:`spark.legacy-size-of-null <../../configs>` is set to false. 
    Otherwise, returns -1 for null input.
