====================
Conversion Functions
====================

Presto will implicity convert numeric and character values to the
correct type if such a conversion is possible. Presto will not convert
between character and numeric types. For example, a query that expects
a varchar will not automatically convert a bigint value to an
equivalent varchar.

When necessary, values can be explicitly cast to a particular type.

Conversion Functions
--------------------

.. function:: CAST(value AS type) -> type

Explicitly cast a value as a type. This can be used to cast a
varchar to a numeric value type and vice versa.
