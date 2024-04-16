=================
Decimal Operators
=================

When calculating the result precision and scale of arithmetic operators,
the formulas follow Hive which is based on the SQL standard and MS SQL:

https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf

https://msdn.microsoft.com/en-us/library/ms190476.aspx

Addition and Subtraction
------------------------

::

	p = max(p1 - s1, p2 - s2) + max(s1, s2) + 1
	s = max(s1, s2)

Multiplication
--------------

::

	p = p1 + p2 + 1
	s = s1 + s2

Division
--------

::

    p = p1 - s1 + s2 + max(6, s1 + p2 + 1)
    s = max(6, s1 + p2 + 1)

For above arithmetic operators, when the precision of result exceeds 38,
caps p at 38 and reduces the scale, in order to prevent the truncation of
the integer part of the decimals. Below formula illustrates how the result
precision and scale are adjusted.

::

    precision = 38
    scale = max(38 - (p - s), min(s, 6))

Users experience runtime errors when the actual result cannot be represented
with the calculated decimal type.

Decimal Functions
-----------------

.. spark:function:: unscaled_value(x) -> bigint

    Return the unscaled bigint value of a short decimal ``x``.
    Supported type is: SHORT_DECIMAL.

Decimal Special Forms
---------------------

.. spark:function:: make_decimal(x[, nullOnOverflow]) -> decimal

    Create ``decimal`` of requsted precision and scale from an unscaled bigint value ``x``.
    By default, the value of ``nullOnOverflow`` is true, and null will be returned when ``x`` is too large for the result precision.
    Otherwise, exception will be thrown when ``x`` overflows.

.. spark:function:: decimal_round(decimal[, scale]) -> [decimal]

    Returns ``decimal`` rounded to a new scale using HALF_UP rounding mode. In HALF_UP rounding, the digit 5 is rounded up.
    ``scale`` is the new scale to be rounded to. It is 0 by default, and integer in [INT_MIN, INT_MAX] is allowed to be its value.
    When the absolute value of scale exceeds the maximum precision of long decimal (38), the round logic is equivalent to the case where it is 38 as we cannot exceed the maximum precision. 
    The result precision and scale are decided with the precision and scale of input ``decimal`` and ``scale``.
    After rounding we may need one more digit in the integral part.
    
    ::
        SELECT (round(cast (9.9 as decimal(2, 1)), 0)); -- decimal 10
        SELECT (round(cast (99 as decimal(2, 0)), -1)); -- decimal 100

    When ``scale`` is negative, we need to adjust ``-scale`` number of digits before the decimal point,
    which means we need at least ``-scale + 1`` digits after rounding, and the result scale is 0.

    ::

        SELECT round(cast (0.856 as DECIMAL(3, 3)), -1); -- decimal 0
        SELECT round(cast (85.6 as DECIMAL(3, 1)), -1); -- decimal 90
        SELECT round(cast (85.6 as DECIMAL(3, 1)), -2); -- decimal 100
        SELECT round(cast (85.6 as DECIMAL(3, 1)), -99);  -- decimal 0
        SELECT round(cast (12345678901234.56789 as DECIMAL(32, 5)), -9); -- decimal 12346000000000

    When ``scale`` is 0, the result scale is 0.

    ::

        SELECT round(cast (85.6 as DECIMAL(3, 1))); -- decimal 86
        SELECT round(cast (0.856 as DECIMAL(3, 3)), 0); -- decimal 1

    When ``scale`` is positive, the result scale is the minor one of input scale and ``scale``.
    The result precision is decided with the number of integral digits and the result scale, but cannot exceed the max precision of decimal.

    ::

        SELECT round(cast (85.681 as DECIMAL(5, 3)), 1); -- decimal 85.7
        SELECT round(cast (85.681 as DECIMAL(5, 3)), 999); -- decimal 85.681
        SELECT round(cast (0.1234567890123456789 as DECIMAL(19, 19)), 14); -- decimal 0.12345678901235
