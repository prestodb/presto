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

    * NaN if the argument is NaN,
    * 1 if the argument is +Infinity,
    * -1 if the argument is -Infinity.

.. function:: sqrt(x) -> double

    Returns the square root of ``x``.

.. function:: to_base(x, radix) -> varchar

    Returns the base-``radix`` representation of ``x``.

.. function:: truncate(x) -> double

    Returns ``x`` rounded to integer by dropping digits after decimal point.

.. function:: truncate(x, n) -> double

    Returns ``x`` truncated to ``n`` decimal places.
    ``n`` can be negative to truncate ``n`` digits left of the decimal point.

    Example:
    ``truncate(REAL '12.333', -1)`` -> result is 10.0
    ``truncate(REAL '12.333', 0)``  -> result is 12.0
    ``truncate(REAL '12.333', 1)``  -> result is 12.3

.. function:: width_bucket(x, bound1, bound2, n) -> bigint

    Returns the bin number of ``x`` in an equi-width histogram with the
    specified ``bound1`` and ``bound2`` bounds and ``n`` number of buckets.

.. function:: width_bucket(x, bins) -> bigint

    Returns the bin number of ``x`` according to the bins specified by the
    array ``bins``. The ``bins`` parameter must be an array of doubles and is
    assumed to be in sorted ascending order.

Probability Functions: cdf
-----------------------

.. function:: beta_cdf(a, b, value) -> double

    Compute the Beta cdf with given a, b parameters:  P(N < value; a, b).
    The a, b parameters must be positive real numbers and value must be a real value (all of type DOUBLE).
    The value must lie on the interval [0, 1].

.. function:: binomial_cdf(numberOfTrials, successProbability, value) -> double

    Compute the Binomial cdf with given numberOfTrials and successProbability (for a single trial):  P(N < value).
    The successProbability must be real value in [0, 1], numberOfTrials and value must be
    positive integers with numberOfTrials greater or equal to value.

.. function:: cauchy_cdf(median, scale, value) -> double

    Compute the Cauchy cdf with given parameters median and scale (gamma): P(N; median, scale).
    The scale parameter must be a positive double. The value parameter must be a double on the interval [0, 1].

.. function:: chi_squared_cdf(df, value) -> double

    Compute the Chi-square cdf with given df (degrees of freedom) parameter:  P(N < value; df).
    The df parameter must be a positive real number, and value must be a non-negative real value (both of type DOUBLE).

.. function:: normal_cdf(mean, sd, value) -> double

    Compute the Normal cdf with given mean and standard deviation (sd):  P(N < value; mean, sd).
    The mean and value must be real values and the standard deviation must be a real
    and positive value (all of type DOUBLE).

.. function:: poisson_cdf(lambda, value) -> double

    Compute the Poisson cdf with given lambda (mean) parameter:  P(N <= value; lambda).
    The lambda parameter must be a positive real number (of type DOUBLE) and value must be a non-negative integer.

.. function:: weibull_cdf(a, b, value) -> double

    Compute the Weibull cdf with given parameters a, b: P(N <= value). The ``a``
    and ``b`` parameters must be positive doubles and ``value`` must also be a double.


Probability Functions: inverse_cdf
-----------------------

.. function:: inverse_beta_cdf(a, b, p) -> double

    Compute the inverse of the Beta cdf with given a, b parameters for the cumulative
    probability (p): P(N < n). The a, b parameters must be positive real values (all of type DOUBLE).
    The probability p must lie on the interval [0, 1].

.. function:: inverse_binomial_cdf(numberOfTrials, successProbability, p) -> int

    Compute the inverse of the Binomial cdf with given numberOfTrials and successProbability (of a single trial) the
    cumulative probability (p):  P(N <= n).
    The successProbability and p must be real values in [0, 1] and the numberOfTrials must be
    a positive integer.

.. function:: inverse_cauchy_cdf(median, scale, p) -> double

    Compute the inverse of the Cauchy cdf with given parameters median and scale (gamma) for the probability p.
    The scale parameter must be a positive double. The probability p must be a double on the interval [0, 1].

.. function:: inverse_chi_squared_cdf(df, p) -> double

    Compute the inverse of the Chi-square cdf with given df (degrees of freedom) parameter for the cumulative
    probability (p): P(N < n). The df parameter must be positive real values.
    The probability p must lie on the interval [0, 1].

.. function:: inverse_normal_cdf(mean, sd, p) -> double

    Compute the inverse of the Normal cdf with given mean and standard
    deviation (sd) for the cumulative probability (p): P(N < n). The mean must be
    a real value and the standard deviation must be a real and positive value (both of type DOUBLE).
    The probability p must lie on the interval (0, 1).

.. function:: inverse_poisson_cdf(lambda, p) -> integer

    Compute the inverse of the Poisson cdf with given lambda (mean) parameter for the cumulative
    probability (p). It returns the value of n so that: P(N <= n; lambda) = p.
    The lambda parameter must be a positive real number (of type DOUBLE).
    The probability p must lie on the interval [0, 1).

.. function:: inverse_weibull_cdf(a, b, p) -> double

    Compute the inverse of the Weibull cdf with given parameters ``a``, ``b`` for the probability ``p``.
    The ``a``, ``b`` parameters must be positive double values. The probability ``p`` must be a double
    on the interval [0, 1].


Statistical Functions
-----------------------

.. function:: wilson_interval_lower(successes, trials, z) -> double

    Returns the lower bound of the Wilson score interval of a Bernoulli trial process
    at a confidence specified by the z-score ``z``.

.. function:: wilson_interval_upper(successes, trials, z) -> double

    Returns the upper bound of the Wilson score interval of a Bernoulli trial process
    at a confidence specified by the z-score ``z``.

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
