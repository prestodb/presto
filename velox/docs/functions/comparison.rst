=====================================
Comparison Functions
=====================================

Like most other Presto functions, these functions return null if any argument
is null. These functions support all scalar input types. All arguments must be
of the same type.

.. function:: eq(x, y) -> boolean

    Returns true if x is equal to y.

.. function:: gt(x, y) -> boolean

    Returns true if x is greater than y.

.. function:: gte(x, y) -> boolean

    Returns true if x is greater than or equal to y.

.. function:: lt(x, y) -> boolean

    Returns true if x is less than y.

.. function:: lte(x, y) -> boolean

    Returns true if x is less than or equal to y.

.. function:: neq(x, y) -> boolean

    Returns true if x is not equal to y.

GREATEST and LEAST
------------------

Like most other Presto functions, these functions return null if any argument
is null.

The following types are supported: DOUBLE, BIGINT, VARCHAR, TIMESTAMP, DATE

.. function:: least(value1, value2, ..., valueN) -> [same as input]

    Returns the smallest of the provided values.

.. function:: greatest(value1, value2, ..., valueN) -> [same as input]

    Returns the largest of the provided values.
