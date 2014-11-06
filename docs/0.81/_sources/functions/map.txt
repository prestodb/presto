.. _map_functions:

===========================
Map Functions and Operators
===========================

Subscript Operator: []
----------------------

The ``[]`` operator is used to retrieve the value corresponding to a given key from a map::

    SELECT name_to_age_map['Bob'] AS bob_age

Map Functions
-------------

.. function:: cardinality(x) -> bigint
    :noindex:

    Returns the cardinality (size) of the map ``x``.

.. function:: map_keys(x<K,V>) -> array<K>

    Returns all the keys in the map ``x``.

.. function:: map_values(x<K,V>) -> array<V>

    Returns all the values in the map ``x``.
