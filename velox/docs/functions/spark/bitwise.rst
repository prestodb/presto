=================
Bitwise Functions
=================

.. spark:function:: bitwise_and(x, y) -> [same as input]

    Returns the bitwise AND of ``x`` and ``y`` in 2's complement representation. 
    Corresponds to Spark's operator ``&``.

.. spark:function:: bitwise_or(x, y) -> [same as input]

    Returns the bitwise OR of ``x`` and ``y`` in 2's complement representation.
    Corresponds to Spark's operator ``^``.

.. spark:function:: shiftleft(x, n) -> [same as x]

    Returns x bitwise left shifted by n bits. Supported types for 'x' are INTEGER and BIGINT.

.. spark:function:: shiftright(x, n) -> [same as x]

    Returns x bitwise right shifted by n bits. Supported types for 'x' are INTEGER and BIGINT.