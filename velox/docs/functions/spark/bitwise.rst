=================
Bitwise Functions
=================

.. spark:function:: bitwise_and(x, y) -> [same as input]

    Returns the bitwise AND of ``x`` and ``y`` in 2's complement representation. 
    Corresponds to Spark's operator ``&``.
    Supported types are: TINYINT, SMALLINT, INTEGER and BIGINT.

.. spark:function:: bitwise_not(x) -> [same as input]

    Returns the bitwise NOT of ``x`` in 2's complement representation.
    Corresponds to Spark's operator ``~``.
    Supported types are: TINYINT, SMALLINT, INTEGER and BIGINT.

.. spark:function:: bitwise_or(x, y) -> [same as input]

    Returns the bitwise OR of ``x`` and ``y`` in 2's complement representation.
    Corresponds to Spark's operator ``|``.
    Supported types are: TINYINT, SMALLINT, INTEGER and BIGINT.

.. spark:function:: bitwise_xor(x, y) -> [same as input]

    Returns the bitwise exclusive OR of ``x`` and ``y`` in 2's complement representation.
    Corresponds to Spark's operator ``^``.
    Supported types are: TINYINT, SMALLINT, INTEGER and BIGINT.

.. spark:function:: bit_count(x) -> integer

    Returns the number of bits that are set in the argument ``x`` as an unsigned 64-bit integer,
    or NULL if the argument is NULL.
    Supported types are: BOOLEAN, TINYINT, SMALLINT, INTEGER and BIGINT.

.. spark:function:: bit_get(x, pos) -> tinyint

    Returns the value of the bit (0 or 1) at the specified position.
    Supported types of ``x`` are: TINYINT, SMALLINT, INTEGER and BIGINT.
    Supported types of ``pos`` are: INTEGER.
    The positions are numbered from right to left, starting at zero.
    The value of 'pos' argument must be between 0 and number of bits in 'x' - 1.
    Invalid 'pos' values result in an error.

.. spark:function:: shiftleft(x, n) -> [same as x]

    Returns x bitwise left shifted by n bits. Supported types for 'x' are INTEGER and BIGINT.

.. spark:function:: shiftright(x, n) -> [same as x]

    Returns x bitwise right shifted by n bits. Supported types for 'x' are INTEGER and BIGINT.