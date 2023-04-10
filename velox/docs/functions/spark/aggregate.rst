===================
Aggregate Functions
===================

Aggregate functions operate on a set of values to compute a single result.

General Aggregate Functions
---------------------------

.. spark:function:: bit_xor(x) -> bigint

    Returns the bitwise XOR of all non-null input values, or null if none.

.. spark:function:: last(x) -> x

    Returns the last value of `x` for a group of rows.

.. spark:function:: last(x, ignoreNull) -> x

    Returns the last value of `x` for a group of rows. If `ignoreNull` is true, returns only non-null values.
