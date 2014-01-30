====================================
Mathematical Functions and Operators
====================================

Mathematical Operators
----------------------

======== ===========
Operator Description
======== ===========
``+``    Addition
``-``    Subtraction
``*``    Multiplication
``/``    Division (integer division performs truncation)
``%``    Modulus (remainder)
======== ===========

Mathematical Functions
----------------------

.. function:: abs(x) -> [same as input]

    Returns the absolute value of ``x``.

.. function:: cbrt(x) -> double

    Returns the cube root of ``x``.

.. function:: ceil(x) -> [same as input]

    This is an alias for :func:`ceiling`.

.. function:: ceiling(x) -> [same as input]

    Returns ``x`` rounded up to the nearest integer.

.. function:: e() -> double

    Returns the constant Euler's number.

.. function:: exp(x) -> double

    Returns Euler's number raised to the power of ``x``.

.. function:: floor(x) -> [same as input]

    Returns ``x`` rounded down to the nearest integer.

.. function:: ln(x) -> double

    Returns the natural logarithm of ``x``.

.. function:: log2(x) -> double

    Returns the base 2 logarithm of ``x``.

.. function:: log10(x) -> double

    Returns the base 10 logarithm of ``x``.

.. function:: log(x, b) -> double

    Returns the base ``b`` logarithm of ``x``.

.. function:: mod(n, m) -> [same as input]

    Returns the modulus (remainder) of ``n`` divided by ``m``.

.. function:: pi() -> double

    Returns the constant Pi.

.. function:: pow(x, p) -> double

    Returns ``x`` raised to the power of ``p``.

.. function:: round(x) -> [same as input]

    Returns ``x`` rounded to the nearest integer.

.. function:: round(x, d) -> [same as input]

    Returns ``x`` rounded to ``d`` decimal places.

.. function:: sqrt(x) -> double

    Returns the square root of ``x``.

Trigonometric Functions
-----------------------

All trigonometric function arguments are expressed in radians.

.. function:: acos(x) -> double

    Returns the arc cosine of ``x``.

.. function:: asin(x) -> double

    Returns the arc sine of ``x``.

.. function:: atan(x) -> double

    Returns the arc tangent of ``x``.

.. function:: atan2(y, x) -> double

    Returns the arc tangent of ``y / x``.

.. function:: cos(x) -> double

    Returns the cosine of ``x``.

.. function:: cosh(x) -> double

    Returns the hyperbolic cosine of ``x``.

.. function:: sin(x) -> double

    Returns the sine of ``x``.

.. function:: tan(x) -> double

    Returns the tangent of ``x``.

.. function:: tanh(x) -> double

    Returns the hyperbolic tangent of ``x``.

Floating Point Functions
------------------------

.. function:: infinity() -> double

    Returns the constant representing positive infinity.

.. function:: is_finite(x) -> boolean

    Determine if ``x`` is finite.

.. function:: is_infinite(x) -> boolean

    Determine if ``x`` is infinite.

.. function:: is_nan(x) -> boolean

    Determine if ``x`` is not-a-number.

.. function:: nan() -> double

    Returns the constant representing not-a-number.
