===================
Aggregate Functions
===================

Aggregate functions operate on a set of values to compute a single result.

Except for :func:`count`, :func:`count_if`, :func:`max_by`, :func:`min_by` and
:func:`approx_distinct`, all of these aggregate functions ignore null values
and return null for no input rows or when all values are null. For example,
:func:`sum` returns null rather than zero and :func:`avg` does not include null
values in the count. The ``coalesce`` function can be used to convert null into
zero.

Some aggregate functions such as :func:`array_agg` produce different results
depending on the order of input values.

General Aggregate Functions
---------------------------

.. function:: arbitrary(x) -> [same as input]

    Returns an arbitrary non-null value of ``x``, if one exists.

.. function:: array_agg(x) -> array<[same as input]>

    Returns an array created from the input ``x`` elements.

.. function:: avg(x) -> double|real

    Returns the average (arithmetic mean) of all input values.
    When x is of type REAL, the result type is REAL.
    For all other input types, the result type is DOUBLE.

.. function:: bool_and(boolean) -> boolean

    Returns ``TRUE`` if every input value is ``TRUE``, otherwise ``FALSE``.

.. function:: bool_or(boolean) -> boolean

    Returns ``TRUE`` if any input value is ``TRUE``, otherwise ``FALSE``.

.. function:: checksum(x) -> varbinary

    Returns an order-insensitive checksum of the given values.

.. function:: count(*) -> bigint

    Returns the number of input rows.

.. function:: count(x) -> bigint

    Returns the number of non-null input values.

.. function:: count_if(x) -> bigint

    Returns the number of ``TRUE`` input values.
    This function is equivalent to ``count(CASE WHEN x THEN 1 END)``.

.. function:: every(boolean) -> boolean

    This is an alias for :func:`bool_and`.

.. function:: histogram(x)

    Returns a map containing the count of the number of times
    each input value occurs. Supports integral, floating-point,
    boolean, timestamp, and date input types.

.. function:: max_by(x, y) -> [same as x]

    Returns the value of ``x`` associated with the maximum value of ``y`` over all input values.

.. function:: min_by(x, y) -> [same as x]

    Returns the value of ``x`` associated with the minimum value of ``y`` over all input values.

.. function:: max(x) -> [same as input]

    Returns the maximum value of all input values.

.. function:: min(x) -> [same as input]

    Returns the minimum value of all input values.

.. function:: sum(x) -> [same as input]

    Returns the sum of all input values.

Bitwise Aggregate Functions
---------------------------

.. function:: bitwise_and_agg(x) -> bigint

    Returns the bitwise AND of all input values in 2's complement representation.

.. function:: bitwise_or_agg(x) -> bigint

    Returns the bitwise OR of all input values in 2's complement representation.

Map Aggregate Functions
-----------------------

.. function:: map_agg(key, value) -> map(K,V)

    Returns a map created from the input ``key`` / ``value`` pairs.

.. function:: map_union(map(K,V)) -> map(K,V)

    Returns the union of all the input ``maps``.
    If a ``key`` is found in multiple input ``maps``,
    that ``key’s`` ``value`` in the resulting ``map`` comes from an arbitrary input ``map``.

Approximate Aggregate Functions
-------------------------------

.. function:: approx_distinct(x) -> bigint

    Returns the approximate number of distinct input values.
    This function provides an approximation of ``count(DISTINCT x)``.
    Zero is returned if all input values are null.

    This function should produce a standard error of 2.3%, which is the
    standard deviation of the (approximately normal) error distribution over
    all possible sets. It does not guarantee an upper bound on the error for
    any specific input set.

.. function:: approx_distinct(x, e) -> bigint

    Returns the approximate number of distinct input values.
    This function provides an approximation of ``count(DISTINCT x)``.
    Zero is returned if all input values are null.

    This function should produce a standard error of no more than ``e``, which
    is the standard deviation of the (approximately normal) error distribution
    over all possible sets. It does not guarantee an upper bound on the error
    for any specific input set. The current implementation of this function
    requires that ``e`` be in the range of ``[0.0040625, 0.26000]``.

.. function:: approx_percentile(x, percentage) -> [same as x]

    Returns the approximate percentile for all input values of ``x`` at the
    given ``percentage``. The value of ``percentage`` must be between zero and
    one and must be constant for all input rows.

.. function:: approx_percentile(x, percentage, accuracy) -> [same as x]

    As ``approx_percentile(x, percentage)``, but with a maximum rank
    error of ``accuracy``. The value of ``accuracy`` must be between
    zero and one (exclusive) and must be constant for all input rows.
    Note that a lower "accuracy" is really a lower error threshold,
    and thus more accurate.  The default accuracy is 0.0133.  The
    underlying implementation is KLL sketch thus has a stronger
    guarantee for accuracy than T-Digest.

.. function:: approx_percentile(x, w, percentage) -> [same as x]

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at the percentage ``p``. The weight must be
    an integer value of at least one. It is effectively a replication count for
    the value ``x`` in the percentile set. The value of ``p`` must be between
    zero and one and must be constant for all input rows.

.. function:: approx_percentile(x, w, percentage, accuracy) -> [same as x]

    As ``approx_percentile(x, w, percentage)``, but with a maximum
    rank error of ``accuracy``.

Statistical Aggregate Functions
-------------------------------

.. function:: corr(y, x) -> double

    Returns correlation coefficient of input values.

.. function:: covar_pop(y, x) -> double

    Returns the population covariance of input values.

.. function:: covar_samp(y, x) -> double

    Returns the sample covariance of input values.

.. function:: stddev(x) -> double

    This is an alias for stddev_samp().

.. function:: stddev_pop(x) -> double

    Returns the population standard deviation of all input values.

.. function:: stddev_samp(x) -> double

    Returns the sample standard deviation of all input values.

.. function:: variance(x) -> double

    This is an alias for var_samp().

.. function:: var_pop(x) -> double

    Returns the population variance of all input values.

.. function:: var_samp(x) -> double

    Returns the sample variance of all input values.
