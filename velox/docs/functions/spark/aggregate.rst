===================
Aggregate Functions
===================

Aggregate functions operate on a set of values to compute a single result.

General Aggregate Functions
---------------------------

.. spark:function:: bit_xor(x) -> bigint

    Returns the bitwise XOR of all non-null input values, or null if none.

.. spark:function:: first(x) -> x

    Returns the first value of `x`.

.. spark:function:: first_ignore_null(x) -> x

    Returns the first non-null value of `x`.

.. spark:function:: last(x) -> x

    Returns the last value of `x`.

.. spark:function:: last_ignore_null(x) -> x

    Returns the last non-null value of `x`.
