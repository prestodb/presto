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
    Returns ``x`` otherwise. ``low`` must be less than or equal to ``high``.

.. function:: degrees(x) -> double

    Converts angle x in radians to degrees.

.. function:: divide(x, y) -> [same as x]

    Returns the results of dividing x by y. The types of x and y must be the same.
    The result of dividing by zero depends on the input types. For integral types,
    division by zero results in an error. For floating point types,  division by
    zero returns positive infinity if x is greater than zero, negative infinity if
    x if less than zero and NaN if x is equal to zero.

.. function:: e() -> double

    Returns the value of Euler's Constant.

.. function:: exp(x) -> double

    Returns Euler's number raised to the power of ``x``.

.. function:: floor(x) -> [same as x]

    Returns ``x`` rounded down to the nearest integer.

.. function:: from_base(string, radix) -> bigint

    Returns the value of ``string`` interpreted as a base-``radix`` number. ``radix`` must be between 2 and 36.

.. function:: ln(x) -> double

    Returns the natural logarithm of ``x``.

.. function:: log2(x) -> double

    Returns the base 2 logarithm of ``x``.

.. function:: log10(x) -> double

    Returns the base 10 logarithm of ``x``.

.. function:: minus(x, y) -> [same as x]

    Returns the result of subtracting y from x. The types of x and y must be the same.
    For integral types, overflow results in an error.

.. function:: mod(n, m) -> [same as n]

    Returns the modulus (remainder) of ``n`` divided by ``m``.

.. function:: multiply(x, y) -> [same as x]

    Returns the result of multiplying x by y. The types of x and y must be the same.
    For integral types, overflow results in an error.

.. function:: negate(x) -> [same as x]

    Returns the additive inverse of x, e.g. the number that, when added to x, yields zero.

.. function:: pi() -> double

    Returns the value of Pi.

.. function:: plus(x, y) -> [same as x]

    Returns the result of adding x to y. The types of x and y must be the same.
    For integral types, overflow results in an error.

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

.. function:: to_base(x, radix) -> varchar

    Returns the base-``radix`` representation of ``x``. ``radix`` must be between 2 and 36.

.. function:: width_bucket(x, bound1, bound2, n) -> bigint

    Returns the bin number of ``x`` in an equi-width histogram with the
    specified ``bound1`` and ``bound2`` bounds and ``n`` number of buckets.

.. function:: width_bucket(x, bins) -> bigint

    Returns the zero-based bin number of ``x`` according to the bins specified
    by the array ``bins``. The ``bins`` parameter must be an array of doubles and
    is assumed to be in sorted ascending order.

    For example, if ``bins`` is ``ARRAY[0, 2, 4]``, then we have four bins:
    ``(-infinity(), 0)``, ``[0, 2)``, ``[2, 4)`` and ``[4, infinity())``.

.. function:: sign(x) -> [same as x]

    Returns the signum function of ``x``. For both integer and floating point arguments, it returns:
    * 0 if the argument is 0,
    * 1 if the argument is greater than 0,
    * -1 if the argument is less than 0.

    For double arguments, the function additionally return:
    * NaN if the argument is NaN,
    * 1 if the argument is +Infinity,
    * -1 if the argument is -Infinity.


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


====================================
Floating Point Functions
====================================

.. function:: infinity() -> double

    Returns the constant representing positive infinity.

.. function:: is_finite(x) -> boolean

    Determine if x is finite.

.. function:: is_infinite(x) -> boolean

    Determine if x is infinite.

.. function:: is_nan(x) -> boolean

    Determine if x is not-a-number.

.. function:: nan() -> double

    Returns the constant representing not-a-number.
