=====================================
Comparison Functions
=====================================

GREATEST and LEAST
-----------------
These functions are not in the SQL standard, but are a common extension.
Like most other functions in Presto, they return null if any argument is null.
Note that in some other databases, such as PostgreSQL, they only return null
if all arguments are null.
The following types are supported: DOUBLE, BIGINT, VARCHAR, TIMESTAMP

.. function:: least(value1, value2, ..., valueN) -> [same as input]

    Returns the smallest of the provided values.

.. function:: greatest(value1, value2, ..., valueN) -> [same as input]

    Returns the largest of the provided values.
