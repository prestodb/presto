=====================================
Comparison Functions
=====================================

.. spark:function:: between(x, min, max) -> boolean

    Returns true if x is within the specified [min, max] range
    inclusive. The types of all arguments must be the same.
    Supported types are: TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, REAL.

.. spark:function:: equalnullsafe(x, y) -> boolean

    Returns true if x is equal to y. Supports all scalar types. The
    types of x and y must be the same. Unlike :spark:func:`equalto` returns true if both inputs
    are NULL and false if one of the inputs is NULL.
    Corresponds to Spark's operator ``<=>``.

.. spark:function:: equalto(x, y) -> boolean

    Returns true if x is equal to y. Supports all scalar types. The
    types of x and y must be the same. Corresponds to Spark's operators ``=`` and ``==``.

.. spark:function:: greaterthan(x, y) -> boolean

    Returns true if x is greater than y. Supports all scalar types. The
    types of x and y must be the same. Corresponds to Spark's operator ``>``.

.. spark:function:: greaterthanorequal(x, y) -> boolean

    Returns true if x is greater than y or x is equal to y. Supports all scalar types. The
    types of x and y must be the same. Corresponds to Spark's operator ``>=``.

.. spark:function:: greatest(value1, value2, ..., valueN) -> [same as input]

    Returns the largest of the provided values ignoring nulls. Supports all scalar types. 
    The types of all arguments must be the same. ::

        SELECT greatest(10, 9, 2, 4, 3); -- 10
        SELECT greatest(10, 9, 2, 4, 3, null); -- 10
        SELECT greatest(null, null) - null

.. spark:function:: least(value1, value2, ..., valueN) -> [same as input]

    Returns the smallest of the provided values ignoring nulls. Supports all scalar types.
    The types of all arguments must be the same. ::

        SELECT least(10, 9, 2, 4, 3); -- 2
        SELECT least(10, 9, 2, 4, 3, null); -- 2
        SELECT least(null, null) - null

.. spark:function:: lessthan(x, y) -> boolean

    Returns true if x is less than y. Supports all scalar types. The types
    of x and y must be the same. Corresponds to Spark's operator ``<``.

.. spark:function:: lessthanorequal(x, y) -> boolean

    Returns true if x is less than y or x is equal to y. Supports all scalar types. The
    types of x and y must be the same. Corresponds to Spark's operator ``<=``.

.. spark:function:: notequalto(x, y) -> boolean

    Returns true if x is not equal to y. Supports all scalar types. The types
    of x and y must be the same. Corresponds to Spark's operator ``!=``.



