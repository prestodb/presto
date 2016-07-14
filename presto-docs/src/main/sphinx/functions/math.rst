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

.. function:: cosine_similarity(x, y) -> double

    Returns the cosine similarity between the sparse vectors ``x`` and ``y``::

        SELECT cosine_similarity(MAP(ARRAY['a'], ARRAY[1.0]), MAP(ARRAY['a'], ARRAY[2.0])); -- 1.0

.. function:: degrees(x) -> double

    Converts angle ``x`` in radians to degrees.

.. function:: e() -> double

    Returns the constant Euler's number.

.. function:: exp(x) -> double

    Returns Euler's number raised to the power of ``x``.

.. function:: floor(x) -> [same as input]

    Returns ``x`` rounded down to the nearest integer.

.. function:: from_base(string, radix) -> bigint

    Returns the value of ``string`` interpreted as a base-``radix`` number.

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

    This is an alias for :func:`power`.

.. function:: power(x, p) -> double

    Returns ``x`` raised to the power of ``p``.

.. function:: radians(x) -> double

    Converts angle ``x`` in degrees to radians.

.. function:: rand() -> double

    This is an alias for :func:`random()`.

.. function:: random() -> double

    Returns a pseudo-random value in the range 0.0 <= x < 1.0.

.. function:: random(n) -> [same as input]

    Returns a pseudo-random number between 0 and n (exclusive).

.. function:: round(x) -> [same as input]

    Returns ``x`` rounded to the nearest integer.

.. function:: round(x, d) -> [same as input]

    Returns ``x`` rounded to ``d`` decimal places.

.. function:: sign(x) -> [same as input]

    Returns the signum function of ``x``, that is:

    * 0 if the argument is 0,
    * 1 if the argument is greater than 0,
    * -1 if the argument is less than 0.

    For double arguments, the function additionally returns:

    * NaN if tha argument is NaN,
    * 1 if the argument is +Infinity,
    * -1 if the argument is -Infinity.

.. function:: sqrt(x) -> double

    Returns the square root of ``x``.

.. function:: to_base(x, radix) -> varchar

    Returns the base-``radix`` representation of ``x``.

.. function:: truncate(x) -> double

    Returns ``x`` rounded to integer by dropping digits after decimal point.

.. function:: width_bucket(x, bound1, bound2, n) -> bigint

    Returns the bin number of ``x`` in an equi-width histogram with the
    specified ``bound1`` and ``bound2`` bounds and ``n`` number of buckets.

.. function:: width_bucket(x, bins) -> bigint

    Returns the bin number of ``x`` according to the bins specified by the
    array ``bins``. The ``bins`` parameter must be an array of doubles and is
    assumed to be in sorted ascending order.

Trigonometric Functions
-----------------------

All trigonometric function arguments are expressed in radians.
See unit conversion functions :func:`degrees` and :func:`radians`.

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
