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

.. function:: bitwise_shift_left(x, shift, bits) -> bigint

    Left shift operation on ``x`` (treated as ``bits``-bit integer)
    shifted by ``shift``::

        SELECT bitwise_shift_left(7, 2, 4); -- 12
        SELECT bitwise_shift_left(7, 2, 64); -- 28

.. function:: bitwise_logical_shift_right(x, shift, bits) -> bigint

    Logical right shift operation on ``x`` (treated as ``bits``-bit integer)
    shifted by ``shift``::

        SELECT bitwise_logical_shift_right(7, 2, 4); -- 1
        SELECT bitwise_logical_shift_right(-8, 2, 5); -- 6

.. function:: bitwise_arithmetic_shift_right(x, shift) -> bigint

    Arithmetic right shift operation on ``x`` shifted by ``shift`` in 2's complement representation::

        SELECT bitwise_arithmetic_shift_right(-8, 2); -- -2
        SELECT bitwise_arithmetic_shift_right(7, 2); -- 1

Generic Shift Functions
-----------------------

These three functions accept values of integral value types ``TINYINT``, ``SMALLINT``, ``INTEGER`` and ``BIGINT``,
and shift them by the amount given by ``shift``, returning a value of the same integral type.  For all three
functions, the amount to shift is given by the bottom bits of the ``shift`` parameter, and higher bits of the
``shift`` parameter are ignored.

.. function:: bitwise_left_shift(value, shift) -> [same as value]

    Returns the left shifted value of ``value``::

        SELECT bitwise_left_shift(TINYINT '7', 2); -- 28
        SELECT bitwise_left_shift(TINYINT '-7', 2); -- -28

.. function:: bitwise_right_shift(value, shift, digits) -> [same as value]

    Returns the logical right shifted value of ``value``::

        SELECT bitwise_right_shift(TINYINT '7', 2); -- 1
        SELECT bitwise_right_shift(SMALLINT -8, 2); -- 16382

.. function:: bitwise_right_shift_arithmetic(value, shift) -> [same as value]

    Returns the arithmetic right shifted value of ``value``::

        SELECT bitwise_right_shift_arithmetic(BIGINT '-8', 2); -- -2
        SELECT bitwise_right_shift_arithmetic(SMALLINT '7', 2); -- 1

See also :func:`bitwise_and_agg` and :func:`bitwise_or_agg`.
