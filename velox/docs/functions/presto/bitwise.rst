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

.. function:: bitwise_arithmetic_shift_right(x, shift) -> bigint

    Returns the arithmetic right shift operation on ``x`` shifted by ``shift`` in 2’s complement representation.
    ``shift`` must not be negative.

.. function:: bitwise_left_shift(x, shift) -> [same as x]

    Returns the left shifted value of ``x``.
    Supported types of x are: ``TINYINT`` , ``SMALLINT``, ``INTEGER`` and ``BIGINT``.
    ``shift`` is an ``INTEGER``.

.. function:: bitwise_logical_shift_right(x, shift, bits) -> bigint

    Returns the logical right shift operation on ``x`` (treated as ``bits``-bit integer) shifted by ``shift``.
    ``shift`` must not be negative.

.. function:: bitwise_not(x) -> bigint

    Returns the bitwise NOT of ``x`` in 2's complement representation.

.. function:: bitwise_or(x, y) -> bigint

    Returns the bitwise OR of ``x`` and ``y`` in 2's complement representation.

.. function:: bitwise_right_shift(x, shift) -> [same as x]

    Returns the logical right shifted value of ``x``.
    Supported types of x are: ``TINYINT``, ``SMALLINT``, ``INTEGER`` and ``BIGINT``.
    ``shift`` is an ``INTEGER``.

.. function:: bitwise_right_shift_arithmetic(x, shift) -> [same as x]

    Returns the arithmetic right shift value of ``x``.
    Supported types of x are: ``TINYINT``, ``SMALLINT``, ``INTEGER`` and ``BIGINT``.
    ``shift`` is an ``INTEGER``.

.. function:: bitwise_shift_left(x, shift, bits) -> bigint

    Returns the left shift operation on ``x`` (treated as ``bits``-bit integer) shifted by ``shift``.
    ``shift`` must not be negative.

.. function:: bitwise_xor(x, y) -> bigint

    Returns the bitwise XOR of ``x`` and ``y`` in 2's complement representation.
