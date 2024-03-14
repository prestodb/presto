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

.. function:: arbitrary(x) -> [same as x]

    Returns an arbitrary non-null value of ``x``, if one exists.

.. function:: any_value(x) -> [same as x]

    This is an alias for :func:`arbitrary`.

.. function:: array_agg(x) -> array<[same as x]>

    Returns an array created from the input ``x`` elements. Ignores null
    inputs if :doc:`presto.array_agg.ignore_nulls <../../configs>` is set
    to false.

.. function:: avg(x) -> double|real

    Returns the average (arithmetic mean) of all non-null input values.
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
    :noindex:

    Returns the number of non-null input values.

.. function:: count_if(x) -> bigint

    Returns the number of ``TRUE`` input values.
    This function is equivalent to ``count(CASE WHEN x THEN 1 END)``.

.. function:: entropy(c) -> double

    Returns the log-2 entropy of count input-values.

    .. math::

        \mathrm{entropy}(c) = \sum_i \left[ {c_i \over \sum_j [c_j]} \log_2\left({\sum_j [c_j] \over c_i}\right) \right].

    ``c`` must be a ``integer`` column of non-negative values.

    The function ignores any ``NULL`` count. If the sum of non-``NULL`` counts is 0,
    it returns 0.

.. function:: every(boolean) -> boolean

    This is an alias for :func:`bool_and`.

.. function:: histogram(x)

    Returns a map containing the count of the number of times
    each input value occurs. Supports integral, floating-point,
    boolean, timestamp, and date input types.

.. function:: geometric_mean(bigint) -> double
              geometric_mean(double) -> double
              geometric_mean(real) -> real

    Returns the `geometric mean <https://en.wikipedia.org/wiki/Geometric_mean>`_ of all input values.

.. function:: max_by(x, y) -> [same as x]

    Returns the value of ``x`` associated with the maximum value of ``y`` over all input values.
    ``y`` must be an orderable type.

.. function:: max_by(x, y, n) -> array([same as x])
    :noindex:

    Returns n values of ``x`` associated with the n largest values of ``y`` in descending order of ``y``.

.. function:: min_by(x, y) -> [same as x]

    Returns the value of ``x`` associated with the minimum value of ``y`` over all input values.
    ``y`` must be an orderable type.

.. function:: min_by(x, y, n) -> array([same as x])
    :noindex:

    Returns n values of ``x`` associated with the n smallest values of ``y`` in ascending order of ``y``.

.. function:: max(x) -> [same as x]

    Returns the maximum value of all input values.
    ``x`` must not contain nulls when it is complex type.
    ``x`` must be an orderable type.

.. function:: max(x, n) -> array<[same as x]>
    :noindex:

    Returns ``n`` largest values of all input values of ``x``.
    ``n`` must be a positive integer and not exceed 10'000.

.. function:: min(x) -> [same as x]

    Returns the minimum value of all input values.
    ``x`` must not contain nulls when it is complex type.
    ``x`` must be an orderable type.

.. function:: min(x, n) -> array<[same as x]>
    :noindex:

    Returns ``n`` smallest values of all input values of ``x``.
    ``n`` must be a positive integer and not exceed 10'000.

.. function:: multimap_agg(K key, V value) -> map(K,array(V))

    Returns a multimap created from the input ``key`` / ``value`` pairs.
    Each key can be associated with multiple values.

.. function:: reduce_agg(inputValue T, initialState S, inputFunction(S,T,S), combineFunction(S,S,S)) -> S

    Reduces all non-NULL input values into a single value. ``inputFunction``
    will be invoked for each non-NULL input value. If all inputs are NULL, the
    result is NULL. In addition to taking the input value, ``inputFunction``
    takes the current state, initially ``initialState``, and returns the new state.
    ``combineFunction`` will be invoked to combine two states into a new state.
    The final state is returned. Throws an error if ``initialState`` is NULL or
    ``inputFunction`` or ``combineFunction`` returns a NULL.

    Take care when designing ``initialState``, ``inputFunction`` and ``combineFunction``.
    These need to support evaluating aggregation in a distributed manner using partial
    aggregation on many nodes, followed by shuffle over group-by keys, followed by
    final aggregation. Given a set of all possible values of state, make sure that
    combineFunction is `commutative <https://en.wikipedia.org/wiki/Commutative_property>`_
    and `associative <https://en.wikipedia.org/wiki/Associative_property>`_
    operation with initialState as the
    `identity <https://en.wikipedia.org/wiki/Identity_element>`_ value.

     combineFunction(s, initialState) = s for any s

     combineFunction(s1, s2) = combineFunction(s2, s1) for any s1 and s2

     combineFunction(s1, combineFunction(s2, s3)) = combineFunction(combineFunction(s1, s2), s3) for any s1, s2, s3

    In addition, make sure that the following holds for the inputFunction:

     inputFunction(inputFunction(initialState, x), y) = combineFunction(inputFunction(initialState, x), inputFunction(initialState, y)) for any x and y

    Check out `blog post about reduce_agg <https://velox-lib.io/blog/reduce-agg>`_ for more context.

    Note that reduce_agg doesn't support evaluation over sorted inputs.::

        -- Compute sum (for illustration purposes only; use SUM aggregate function in production queries).
        SELECT id, reduce_agg(value, 0, (a, b) -> a + b, (a, b) -> a + b)
        FROM (
            VALUES
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 20),
                (2, 30),
                (2, 40)
        ) AS t(id, value)
        GROUP BY id;
        -- (1, 9)
        -- (2, 90)

        -- Compute product.
        SELECT id, reduce_agg(value, 1, (a, b) -> a * b, (a, b) -> a * b)
        FROM (
            VALUES
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 20),
                (2, 30),
                (2, 40)
        ) AS t(id, value)
        GROUP BY id;
        -- (1, 24)
        -- (2, 24000)

        -- Compute avg (for illustration purposes only; use AVG aggregate function in production queries).
        SELECT id, sum_and_count.sum / sum_and_count.count FROM (
          SELECT id, reduce_agg(value, CAST(row(0, 0) AS row(sum double, count bigint)),
            (s, x) -> CAST(row(s.sum + x, s.count + 1) AS row(sum double, count bigint)),
            (s, s2) -> CAST(row(s.sum + s2.sum, s.count + s2.count) AS row(sum double, count bigint))) AS sum_and_count
          FROM (
               VALUES
                   (1, 2),
                   (1, 3),
                   (1, 4),
                   (2, 20),
                   (2, 30),
                   (2, 40)
           ) AS t(id, value)
           GROUP BY id
        );
        -- (1, 3.0)
        -- (2, 30.0)

.. function:: set_agg(x) -> array<[same as x]>

    Returns an array created from the distinct input ``x`` elements.
    ``x`` must not contain nulls when it is complex type.

.. function:: set_union(array(T)) -> array(T)

    Returns an array of all the distinct values contained in each array of the input.

    Returns an empty array if all input arrays are NULL.

    Example::

        SELECT set_union(elements)
        FROM (
            VALUES
                ARRAY[1, 2, 3],
                ARRAY[2, 3, 4]
        ) AS t(elements);

    Returns ARRAY[1, 2, 3, 4]

.. function:: sum(x) -> [same as x]

    Returns the sum of all input values.

Bitwise Aggregate Functions
---------------------------

.. function:: bitwise_and_agg(x) -> [same as x]

    Returns the bitwise AND of all input values in 2's complement representation.

    Supported types are TINYINT, SMALLINT, INTEGER and BIGINT.

.. function:: bitwise_or_agg(x) -> [same as x]

    Returns the bitwise OR of all input values in 2's complement representation.

    Supported types are TINYINT, SMALLINT, INTEGER and BIGINT.

.. function:: bitwise_xor_agg(x) -> [same as x]

    Returns the bitwise XOR of all input values in 2's complement representation.

    Supported types are TINYINT, SMALLINT, INTEGER and BIGINT.

Map Aggregate Functions
-----------------------

.. function:: map_agg(K key, V value) -> map(K,V)

    Returns a map created from the input ``key`` / ``value`` pairs. Inputs with NULL or duplicate keys are ignored.

.. function:: map_union(map(K,V)) -> map(K,V)

    Returns the union of all the input ``maps``.
    If a ``key`` is found in multiple input ``maps``,
    that ``key’s`` ``value`` in the resulting ``map`` comes from an arbitrary input ``map``.

.. function:: map_union_sum(map(K,V)) -> map(K,V)

    Returns the union of all the input maps summing the values of matching keys in all
    the maps. All null values in the original maps are coalesced to 0.

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
   :noindex:

    Returns the approximate number of distinct input values.
    This function provides an approximation of ``count(DISTINCT x)``.
    Zero is returned if all input values are null.

    This function should produce a standard error of no more than ``e``, which
    is the standard deviation of the (approximately normal) error distribution
    over all possible sets. It does not guarantee an upper bound on the error
    for any specific input set. The current implementation of this function
    requires that ``e`` be in the range of ``[0.0040625, 0.26000]``.

.. function:: approx_most_frequent(buckets, value, capacity) -> map<[same as value], bigint>

    Computes the top frequent values up to ``buckets`` elements approximately.
    Approximate estimation of the function enables us to pick up the frequent
    values with less memory.  Larger ``capacity`` improves the accuracy of
    underlying algorithm with sacrificing the memory capacity.  The returned
    value is a map containing the top elements with corresponding estimated
    frequency.

    The error of the function depends on the permutation of the values and its
    cardinality.  We can set the capacity same as the cardinality of the
    underlying data to achieve the least error.

    ``buckets`` and ``capacity`` must be ``bigint``.  ``value`` can be numeric
    or string type.

    The function uses the stream summary data structure proposed in the paper
    `Efficient computation of frequent and top-k elements in data streams`__
    by A. Metwally, D. Agrawal and A. Abbadi.

__ https://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf

.. function:: approx_percentile(x, percentage) -> [same as x]

    Returns the approximate percentile for all input values of ``x`` at the
    given ``percentage``. The value of ``percentage`` must be between zero and
    one and must be constant for all input rows.

.. function:: approx_percentile(x, percentage, accuracy) -> [same as x]
   :noindex:

    As ``approx_percentile(x, percentage)``, but with a maximum rank
    error of ``accuracy``. The value of ``accuracy`` must be between
    zero and one (exclusive) and must be constant for all input rows.
    Note that a lower "accuracy" is really a lower error threshold,
    and thus more accurate.  The default accuracy is 0.0133.  The
    underlying implementation is KLL sketch thus has a stronger
    guarantee for accuracy than T-Digest.

.. function:: approx_percentile(x, percentages) -> array<[same as x]>
   :noindex:

    Returns the approximate percentile for all input values of ``x`` at each of
    the specified percentages. Each element of the ``percentages`` array must be
    between zero and one, and the array must be constant for all input rows.

.. function:: approx_percentile(x, percentages, accuracy) -> array<[same as x]>
   :noindex:

    As ``approx_percentile(x, percentages)``, but with a maximum rank error of
    ``accuracy``.

.. function:: approx_percentile(x, w, percentage) -> [same as x]
   :noindex:

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at the percentage ``p``. The weight must be
    an integer value of at least one. It is effectively a replication count for
    the value ``x`` in the percentile set. The value of ``p`` must be between
    zero and one and must be constant for all input rows.

.. function:: approx_percentile(x, w, percentage, accuracy) -> [same as x]
   :noindex:

    As ``approx_percentile(x, w, percentage)``, but with a maximum
    rank error of ``accuracy``.

.. function:: approx_percentile(x, w, percentages) -> array<[same as x]>
   :noindex:

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at each of the given percentages specified
    in the array. The weight must be an integer value of at least one. It is
    effectively a replication count for the value ``x`` in the percentile
    set. Each element of the array must be between zero and one, and the array
    must be constant for all input rows.

.. function:: approx_percentile(x, w, percentages, accuracy) -> array<[same as x]>
   :noindex:

    As ``approx_percentile(x, w, percentages)``, but with a maximum rank error
    of ``accuracy``.

Statistical Aggregate Functions
-------------------------------

.. function:: corr(y, x) -> double

    Returns correlation coefficient of input values.

.. function:: covar_pop(y, x) -> double

    Returns the population covariance of input values.

.. function:: covar_samp(y, x) -> double

    Returns the sample covariance of input values.

.. function:: kurtosis(x) -> double

    Returns the excess kurtosis of all input values. Unbiased estimate using
    the following expression:

    .. math::

        \mathrm{kurtosis}(x) = {n(n+1) \over (n-1)(n-2)(n-3)} { \sum[(x_i-\mu)^4] \over \sigma^4} -3{ (n-1)^2 \over (n-2)(n-3) },

   where :math:`\mu` is the mean, and :math:`\sigma` is the standard deviation.

.. function:: regr_avgx(y, x) -> double

    Returns the average of the independent value in a group. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_avgy(y, x) -> double

    Returns the average of the dependent value in a group. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_count(y, x) -> double

    Returns the number of non-null pairs of input values. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_intercept(y, x) -> double

    Returns linear regression intercept of input values. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_r2(y, x) -> double

    Returns the coefficient of determination of the linear regression. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_slope(y, x) -> double

    Returns linear regression slope of input values. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_sxx(y, x) -> double

    Returns the sum of the squares of the independent values in a group. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_sxy(y, x) -> double

    Returns the sum of the product of the dependent and independent values in a group. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_syy(y, x) -> double

    Returns the sum of the squares of the dependent values in a group. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: skewness(x) -> double

    Returns the skewness of all input values.

.. function:: stddev(x) -> double

    This is an alias for :func:`stddev_samp`.

.. function:: stddev_pop(x) -> double

    Returns the population standard deviation of all input values.

.. function:: stddev_samp(x) -> double

    Returns the sample standard deviation of all input values.

.. function:: variance(x) -> double

    This is an alias for :func:`var_samp`.

.. function:: var_pop(x) -> double

    Returns the population variance of all input values.

.. function:: var_samp(x) -> double

    Returns the sample variance of all input values.

Miscellaneous
-------------

.. function:: max_data_size_for_stats(x) -> bigint

    Returns an estimate of the the maximum in-memory size in bytes of ``x``.

.. function:: sum_data_size_for_stats(x) -> bigint

    Returns an estimate of the sum of in-memory size in bytes of ``x``.
