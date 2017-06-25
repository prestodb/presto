=================
Bitwise Functions
=================

.. function:: bit_count(x, bits) -> bigint

    Count the number of bits set in ``x`` (treated as ``bits``-bit signed
    integer) in 2's complement representation::

        SELECT bit_count(9, 64); -- 2
        SELECT bit_count(9, 8); -- 2
        SELECT bit_count(-7, 64); -- 62
        SELECT bit_count(-7, 8); -- 6

.. function:: bitwise_and(x, y) -> bigint

    Returns the bitwise AND of ``x`` and ``y`` in 2's complement representation.

.. function:: bitwise_not(x) -> bigint

    Returns the bitwise NOT of ``x`` in 2's complement representation.

.. function:: bitwise_or(x, y) -> bigint

    Returns the bitwise OR of ``x`` and ``y`` in 2's complement representation.

.. function:: bitwise_xor(x, y) -> bigint

    Returns the bitwise XOR of ``x`` and ``y`` in 2's complement representation.

See also :func:`bitwise_and_agg` and :func:`bitwise_or_agg`.
