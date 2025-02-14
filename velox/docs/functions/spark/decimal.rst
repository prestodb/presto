=================
Decimal Operators
=================

The result precision and scale computation of arithmetic operators contains two stages.
First stage computes precision and scale using formulas based on the SQL standard and Hive when allow-precision-loss is true.
The result may exceed maximum allowed precision of 38.

Second stage caps precision at 38 and either reduces the scale or not depending on allow-precision-loss flag.

For example, addition of decimal(38, 7) and decimal(10, 0) requires precision of 39 and scale of 7.
Since precision exceeds 38 it needs to be capped. When allow-precision-loss, precision is capped at 38 and scale is reduced by 1 to 6.
When allow-precision-loss is false, precision is capped at 38 as well, but scale is kept at 7.
With allow-precision-loss all additions will succeed, but accuracy (number of digits after period) of some operations will be reduced.
Without allow-precision-loss, some additions will return NULL.

For example,

The following queries keep accuracy or return NULL when allow-precision-loss is false:

::

    select cast('1.1232154' as decimal(38, 7)) + cast('1' as decimal(10, 0)); -- 2.123215
    select cast('9999999999999999999999999999999.2345678' as decimal(38, 7)) + cast('1' as decimal(10, 0)); -- NULL

These same operations succeed when allow-precision-loss is true:

::

    select cast('1.1232154' as decimal(38, 7)) + cast('1' as decimal(10, 0)); -- 2.12321, lost the last digit
    select cast('9999999999999999999999999999999.2345678' as decimal(38, 7)) + cast('1' as decimal(10, 0)); -- 10000000000000000000000000000000.234568

Decimal Precision and Scale Computation Formulas
------------------------------------------------

The HiveQL behavior:

https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf

Additionally, the computation of decimal division adapts to the allow-precision-loss flag,
while the decimal addition, subtraction, and multiplication do not.

Addition and Subtraction
~~~~~~~~~~~~~~~~~~~~~~~~

::

	p = max(p1 - s1, p2 - s2) + max(s1, s2) + 1
	s = max(s1, s2)

Multiplication
~~~~~~~~~~~~~~

::

	p = p1 + p2 + 1
	s = s1 + s2

Division
~~~~~~~~
When allow-precision-loss is true:

::

    p = p1 - s1 + s2 + max(6, s1 + p2 + 1)
    s = max(6, s1 + p2 + 1)

When allow-precision-loss is false:

::

    wholeDigits = min(38, p1 - s1 + s2);
    fractionalDigits = min(38, max(6, s1 + p2 + 1));
    p = wholeDigits + fractionalDigits
    s = fractionalDigits

Decimal Precision and Scale Adjustment
--------------------------------------

When allow-precision-loss is true, rounds the decimal part of the result if an exact representation is not possible.
Otherwise, returns NULL.
Notice: some operations succeed if precision loss is allowed and return NULL if not.

For example,

::

    select cast(0.1234567891011 as decimal(38, 18)) * cast(1234.1 as decimal(38, 18));
    -- 152.358023 if allow-precision-loss, NULL otherwise.

Below formula illustrates how the result precision and scale are adjusted.

::

    precision = 38
    scale = max(38 - (p - s), min(s, 6))

When precision loss is not allowed, caps p at 38, and keeps scale as is.
The below formula shows how the precision and scale are adjusted for decimal addition, subtraction, and multiplication.

::

    precision = 38
    scale = min(38, s)

Decimal division uses a different formula:

::

    precision = 38
    scale = fractionalDigits - (wholeDigits + fractionalDigits - 38) / 2 - 1

Returns NULL when the actual result cannot be represented with the calculated decimal type.

Decimal Functions
-----------------
.. spark:function:: ceil(x: decimal(p, s)) -> r: decimal(pr, 0)

    Returns ``x`` rounded up to the type ``decimal(min(38, p - s + min(1, s)), 0)``.

    ::

        SELECT ceil(cast(1.23 as DECIMAL(3, 2))); -- 2 // Output type: decimal(2,0)

.. spark:function:: floor(x: decimal(p, s)) -> r: decimal(pr, 0)

    Returns ``x`` rounded down to the type ``decimal(min(38, p - s + min(1, s)), 0)``.

    ::

        SELECT floor(cast(1.23 as DECIMAL(3, 2))); -- 1 // Output type: decimal(2,0)

.. spark:function:: in(x: decimal(p, s), array(decimal(p, s))) -> boolean

    Returns true if ``x`` matches at least one of the elements of the array.

.. spark:function:: unaryminus(x: decimal(p, s)) -> r: decimal(p, s)

    Returns negated value of x (r = -x). Corresponds to Spark's operator ``-``.

    ::

        SELECT unaryminus(cast(-9999999999999999999.9999999999999999999 as DECIMAL(38, 19))); -- 9999999999999999999.9999999999999999999

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
