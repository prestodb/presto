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

    ``low`` is expected to be less than or equal to ``high``. This expection is not
    verified for performance reasons. Returns ``high`` for all values of ``x``
    when ``low`` is greater than ``high``.

.. function:: cosine_similarity(map(varchar, double), map(varchar, double)) -> double

    Returns the `cosine similarity <https://en.wikipedia.org/wiki/Cosine_similarity>`_ between the vectors represented as map(varchar, double).
    If any input map is empty, the function returns NaN.

        SELECT cosine_similarity(MAP(ARRAY['a'], ARRAY[1.0]), MAP(ARRAY['a'], ARRAY[2.0])); -- 1.0

        SELECT cosine_similarity(MAP(ARRAY['a', 'b'], ARRAY[1.0, 2.0]), MAP(ARRAY['a', 'b'], ARRAY[NULL, 3.0])); -- NULL

        SELECT cosine_similarity(MAP(ARRAY[], ARRAY[]), MAP(ARRAY['a', 'b'], ARRAY[2, 3])); -- NaN

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

.. function:: random(n) -> [same as n]
   :noindex:

    Returns a pseudo-random value in the range 0.0 <= x < n.

.. function:: round(x) -> [same as x]

    Returns ``x`` rounded to the nearest integer.

.. function:: round(x, d) -> [same as x]
   :noindex:

    Returns ``x`` rounded to ``d`` decimal places.

.. function:: secure_rand() -> double

    This is an alias for :func:`secure_random()`.

.. function:: secure_random() -> double

    Returns a cryptographically secure random value in the range 0.0 <= x < 1.0.

.. function:: secure_random(lower, upper) -> [same as input]

    Returns a cryptographically secure random value in the range lower <= x < upper, where lower < upper.

.. function:: sign(x) -> [same as x]

    Returns the signum function of ``x``. For both integer and floating point arguments, it returns:
    * 0 if the argument is 0,
    * 1 if the argument is greater than 0,
    * -1 if the argument is less than 0.

    For double arguments, the function additionally return:
    * NaN if the argument is NaN,
    * 1 if the argument is +Infinity,
    * -1 if the argument is -Infinity.

.. function:: sqrt(x) -> double

    Returns the square root of ``x`` . If ``x`` is negative, ``NaN`` is returned.

.. function:: to_base(x, radix) -> varchar

    Returns the base-``radix`` representation of ``x``. ``radix`` must be between 2 and 36.

.. function:: truncate(x) -> [same as x]

    Returns x rounded to integer by dropping digits after decimal point.
    Supported types of ``x`` are: REAL and DOUBLE.

.. function:: truncate(x, n) -> [same as x]
   :noindex:

    Returns x truncated to n decimal places. n can be negative to truncate n digits left of the decimal point.
    Supported types of ``x`` are: REAL and DOUBLE.
    ``n`` is an INTEGER.

.. function:: width_bucket(x, bound1, bound2, n) -> bigint

    Returns the bin number of ``x`` in an equi-width histogram with the
    specified ``bound1`` and ``bound2`` bounds and ``n`` number of buckets.

.. function:: width_bucket(x, bins) -> bigint
   :noindex:

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


====================================
Probability Functions: cdf
====================================

.. function:: beta_cdf(a, b, value) -> double

    Compute the `Beta cdf <https://en.wikipedia.org/wiki/Beta_distribution>`_ with given a, b parameters:  P(N < value; a, b).
    The a, b parameters must be positive real numbers and value must be a real value (all of type DOUBLE).
    The value must lie on the interval [0, 1].

.. function:: binomial_cdf(numberOfTrials, successProbability, value) -> double

    Compute the Binomial cdf with given numberOfTrials and successProbability (for a single trial):  P(N < value).
    The successProbability must be real value in [0, 1], numberOfTrials and value must be
    positive integers with numberOfTrials greater or equal to value

.. function:: cauchy_cdf(median, scale, value) -> double

    Compute the Cauchy cdf with given parameters median and scale (gamma): P(N; median, scale).
    The scale parameter must be a positive double. The value parameter must be a double on the interval [0, 1].

.. function:: chi_squared_cdf(df, value) -> double

    Compute the Chi-square cdf with given df (degrees of freedom) parameter:  P(N < value; df).
    The df parameter must be a positive real number, and value must be a non-negative real value (both of type DOUBLE).

.. function:: f_cdf(df1, df2, value) -> double

    Compute the F cdf with given df1 (numerator degrees of freedom) and df2 (denominator degrees of freedom) parameters:  P(N < value; df1, df2).
    The numerator and denominator df parameters must be positive real numbers. The value must be a non-negative real number.

.. function:: gamma_cdf(shape, scale, value) -> double

    Compute the Gamma cdf with given shape and scale parameters: P(N < value; shape, scale).
    The shape and scale parameters must be positive real numbers. The value must be a non-negative real number.

.. function:: inverse_normal_cdf(mean, sd, p) -> double

    Compute the inverse of the Normal cdf with given mean and standard
    deviation (sd) for the cumulative probability (p): P(N < n). The mean must be
    a real value and the standard deviation must be a real and positive value (both of type DOUBLE).
    The probability p must lie on the interval (0, 1).

.. function:: laplace_cdf(mean, scale, value) -> double

     Compute the Laplace cdf with given mean and scale parameters: P(N < value; mean, scale).
     The mean and value must be real values and the scale parameter must be a
     positive value (all of type DOUBLE).

.. function:: normal_cdf(mean, sd, value) -> double

    Compute the Normal cdf with given mean and standard deviation (sd): P(N < value; mean, sd).
    The mean and value must be real values and the standard deviation must be a real and
    positive value (all of type DOUBLE).

.. function:: poisson_cdf(lambda, value) -> double

    Compute the Poisson cdf with given lambda (mean) parameter:  P(N <= value; lambda).
    The lambda parameter must be a positive real number (of type DOUBLE) and value must be a non-negative integer.


.. function:: weibull_cdf(a, b, value) -> double

    Compute the Weibull cdf with given parameters a, b: P(N <= value). The ``a``
    and ``b`` parameters must be positive doubles and ``value`` must also be a double.

====================================
Probability Functions: inverse_cdf
====================================

.. function:: inverse_beta_cdf(a, b, p) -> double

    Compute the inverse of the Beta cdf with given a, b parameters for the cumulative
    probability (p): P(N < n). The a, b parameters must be positive real values (all of type DOUBLE).
    The probability p must lie on the interval [0, 1].


====================================
Statistical Functions
====================================

.. function:: wilson_interval_lower(successes, trials, z) -> double

    Returns the lower bound of the Wilson score interval of a Bernoulli trial process
    at a confidence specified by the z-score z.

.. function:: wilson_interval_upper(successes, trials, z) -> double

    Returns the upper bound of the Wilson score interval of a Bernoulli trial process
    at a confidence specified by the z-score z.
