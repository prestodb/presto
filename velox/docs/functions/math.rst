====================================
Mathematical Functions
====================================

.. function:: abs(x) -> [same as x]

    Returns the absolute value of ``x``.

.. function:: cbrt(x) -> double

    Returns the cube root of ``x``.

.. function:: ceil(x) -> [same as x]

    This is an alias for :func:`ceiling`.

.. function:: ceiling(x) -> [same as x]

    Returns ``x`` rounded up to the nearest integer.

.. function:: clamp(x, low, high) -> [same as x]

    Returns ``low`` if ``x`` is less than ``low``. Returns ``high`` if ``x`` is greater than ``high``.
    Returns ``x`` otherwise.

.. function:: exp(x) -> double

    Returns Euler's number raised to the power of ``x``.

.. function:: floor(x) -> [same as x]

    Returns ``x`` rounded down to the nearest integer.

.. function:: ln(x) -> double

    Returns the natural logarithm of ``x``.

.. function:: log2(x) -> double

    Returns the base 2 logarithm of ``x``.

.. function:: log10(x) -> double

    Returns the base 10 logarithm of ``x``.

.. function:: mod(n, m) -> [same as n]

    Returns the modulus (remainder) of ``n`` divided by ``m``.

.. function:: pow(x, p) -> double

    This is an alias for :func:`power`.

.. function:: power(x, p) -> double

    Returns ``x`` raised to the power of ``p``.

.. function:: radians(x) -> double

    Converts angle x in degrees to radians.

.. function:: rand() -> double

    This is an alias for :func:`random()`.

.. function:: random() -> double

    Returns a pseudo-random value in the range 0.0 <= x < 1.0.

.. function:: round(x) -> [same as x]

    Returns ``x`` rounded to the nearest integer.

.. function:: round(x, d) -> [same as x]

    Returns ``x`` rounded to ``d`` decimal places.

.. function:: sqrt(x) -> double

    Returns the square root of ``x`` . If ``x`` is negative, ``NaN`` is returned.

.. function:: width_bucket(x, bound1, bound2, n) -> bigint

    Returns the bin number of ``x`` in an equi-width histogram with the
    specified ``bound1`` and ``bound2`` bounds and ``n`` number of buckets.

.. function:: width_bucket(x, bins) -> bigint

    Returns the zero-based bin number of ``x`` according to the bins specified
    by the array ``bins``. The ``bins`` parameter must be an array of doubles and
    is assumed to be in sorted ascending order.

    For example, if ``bins`` is ``ARRAY[0, 2, 4]``, then we have four bins:
    ``(-infinity(), 0)``, ``[0, 2)``, ``[2, 4)`` and ``[4, infinity())``.


====================================
Trigonometric Functions
====================================

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
