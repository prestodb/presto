=====================================
Comparison Functions
=====================================

.. spark:function:: between(x, min, max) -> boolean

    Returns true if x is within the specified [min, max] range
    inclusive. The types of all arguments must be the same.
    Supported types are: TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, REAL, DECIMAL.

.. spark:function:: equalnullsafe(x, y) -> boolean

    Returns true if ``x`` is equal to ``y``. Supports all scalar and complex types. The
    types of ``x`` and ``y`` must be the same. Unlike :spark:func:`equalto` returns true if both inputs
    are NULL and false if one of the inputs is NULL. Nested nulls are compared as values.
    Corresponds to Spark's operator ``<=>``.
    Note that NaN in Spark is handled differently from standard floating point semantics.
    It is considered larger than any other numeric values. This rule is applied for functions
    "equalnullsafe", "equalto", "greaterthan", "greaterthanorequal", "lessthan", "lessthanorequal". ::

        SELECT equalnullsafe(null, null); -- true
        SELECT equalnullsafe(null, ARRAY[1]); -- false
        SELECT equalnullsafe(ARRAY[1, null], ARRAY[1, null]); -- true

.. spark:function:: equalto(x, y) -> boolean

    Returns true if x is equal to y. Supports all scalar and complex types. The
    types of x and y must be the same. Corresponds to Spark's operators ``=`` and ``==``.
    Returns NULL for any NULL input, but nested nulls are compared as values. ::
    
        SELECT equalto(null, null); -- null
        SELECT equalto(null, ARRAY[1]); -- null
        SELECT equalto(ARRAY[1, null], ARRAY[1, null]); -- true

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

.. spark:function:: isnotnull(x) -> boolean

    Returns true if x is not null, or false otherwise. ::

        SELECT isnotnull(1); -- true

.. spark:function:: isnull(x) -> boolean

    Returns true if x is null, or false otherwise. ::

        SELECT isnull(1); -- false

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

.. spark:function:: decimal_lessthan(x, y) -> boolean

    Returns true if x is less than y. Supports decimal types with different precisions and scales.
    Corresponds to Spark's operator ``<``.

.. spark:function:: decimal_lessthanorequal(x, y) -> boolean

    Returns true if x is less than y or x is equal to y. Supports decimal types with different precisions and scales.
    Corresponds to Spark's operator ``<=``.

.. spark:function:: decimal_equalto(x, y) -> boolean

    Returns true if x is equal to y. Supports decimal types with different precisions and scales.
    Corresponds to Spark's operator ``==``.

.. spark:function:: decimal_notequalto(x, y) -> boolean

    Returns true if x is not equal to y. Supports decimal types with different precisions and scales.
    Corresponds to Spark's operator ``!=``.

.. spark:function:: decimal_greaterthan(x, y) -> boolean

    Returns true if x is greater than y. Supports decimal types with different precisions and scales.
    Corresponds to Spark's operator ``>``.

.. spark:function:: decimal_greaterthanorequal(x, y) -> boolean

    Returns true if x is greater than y or x is equal to y. Supports decimal types with different precisions and scales.
    Corresponds to Spark's operator ``>=``.
