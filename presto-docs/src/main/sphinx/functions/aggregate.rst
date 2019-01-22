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
depending on the order of input values. This ordering can be specified by writing
an :ref:`order-by-clause` within the aggregate function::

    array_agg(x ORDER BY y DESC)
    array_agg(x ORDER BY x, y, z)


General Aggregate Functions
---------------------------

.. function:: arbitrary(x) -> [same as input]

    Returns an arbitrary non-null value of ``x``, if one exists.

.. function:: array_agg(x) -> array<[same as input]>

    Returns an array created from the input ``x`` elements.

.. function:: avg(x) -> double

    Returns the average (arithmetic mean) of all input values.

.. function:: avg(time interval type) -> time interval type

    Returns the average interval length of all input values.

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

.. function:: geometric_mean(x) -> double

    Returns the geometric mean of all input values.

.. function:: max_by(x, y) -> [same as x]

    Returns the value of ``x`` associated with the maximum value of ``y`` over all input values.

.. function:: max_by(x, y, n) -> array<[same as x]>

    Returns ``n`` values of ``x`` associated with the ``n`` largest of all input values of ``y``
    in descending order of ``y``.

.. function:: min_by(x, y) -> [same as x]

    Returns the value of ``x`` associated with the minimum value of ``y`` over all input values.

.. function:: min_by(x, y, n) -> array<[same as x]>

    Returns ``n`` values of ``x`` associated with the ``n`` smallest of all input values of ``y``
    in ascending order of ``y``.

.. function:: max(x) -> [same as input]

    Returns the maximum value of all input values.

.. function:: max(x, n) -> array<[same as x]>

    Returns ``n`` largest values of all input values of ``x``.

.. function:: min(x) -> [same as input]

    Returns the minimum value of all input values.

.. function:: min(x, n) -> array<[same as x]>

    Returns ``n`` smallest values of all input values of ``x``.

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

.. function:: histogram(x) -> map(K,bigint)

    Returns a map containing the count of the number of times each input value occurs.

.. function:: map_agg(key, value) -> map(K,V)

    Returns a map created from the input ``key`` / ``value`` pairs.

.. function:: map_union(x(K,V)) -> map(K,V)

   Returns the union of all the input maps. If a key is found in multiple
   input maps, that key's value in the resulting map comes from an arbitrary input map.

.. function:: multimap_agg(key, value) -> map(K,array(V))

    Returns a multimap created from the input ``key`` / ``value`` pairs.
    Each key can be associated with multiple values.

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

.. function:: approx_percentile(x, percentages) -> array<[same as x]>

    Returns the approximate percentile for all input values of ``x`` at each of
    the specified percentages. Each element of the ``percentages`` array must be
    between zero and one, and the array must be constant for all input rows.

.. function:: approx_percentile(x, w, percentage) -> [same as x]

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at the percentage ``p``. The weight must be
    an integer value of at least one. It is effectively a replication count for
    the value ``x`` in the percentile set. The value of ``p`` must be between
    zero and one and must be constant for all input rows.

.. function:: approx_percentile(x, w, percentage, accuracy) -> [same as x]

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at the percentage ``p``, with a maximum rank
    error of ``accuracy``. The weight must be an integer value of at least one.
    It is effectively a replication count for the value ``x`` in the percentile
    set. The value of ``p`` must be between zero and one and must be constant
    for all input rows. ``accuracy`` must be a value greater than zero and less
    than one, and it must be constant for all input rows.

.. function:: approx_percentile(x, w, percentages) -> array<[same as x]>

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at each of the given percentages specified
    in the array. The weight must be an integer value of at least one. It is
    effectively a replication count for the value ``x`` in the percentile set.
    Each element of the array must be between zero and one, and the array must
    be constant for all input rows.

.. function:: approx_set(x) -> HyperLogLog
    :noindex:

    See :doc:`hyperloglog`.

.. function:: merge(x) -> HyperLogLog
    :noindex:

    See :doc:`hyperloglog`.

.. function:: merge(qdigest(T)) -> qdigest(T)
    :noindex:

    See :doc:`qdigest`.

.. function:: qdigest_agg(x) -> qdigest<[same as x]>
    :noindex:

    See :doc:`qdigest`.

.. function:: qdigest_agg(x, w) -> qdigest<[same as x]>
    :noindex:

    See :doc:`qdigest`.

.. function:: qdigest_agg(x, w, accuracy) -> qdigest<[same as x]>
    :noindex:

    See :doc:`qdigest`.

.. function:: numeric_histogram(buckets, value, weight) -> map<double, double>

    Computes an approximate histogram with up to ``buckets`` number of buckets
    for all ``value``\ s with a per-item weight of ``weight``. The algorithm
    is based loosely on:

    .. code-block:: none

        Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
        J. Machine Learning Research 11 (2010), pp. 849--872.

    ``buckets`` must be a ``bigint``. ``value`` and ``weight`` must be numeric.

.. function:: numeric_histogram(buckets, value) -> map<double, double>

    Computes an approximate histogram with up to ``buckets`` number of buckets
    for all ``value``\ s. This function is equivalent to the variant of
    :func:`numeric_histogram` that takes a ``weight``, with a per-item weight of ``1``.

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

    .. code-block:: none

        kurtosis(x) = n(n+1)/((n-1)(n-2)(n-3))sum[(x_i-mean)^4]/stddev(x)^4-3(n-1)^2/((n-2)(n-3))

.. function:: regr_intercept(y, x) -> double

    Returns linear regression intercept of input values. ``y`` is the dependent
    value. ``x`` is the independent value.

.. function:: regr_slope(y, x) -> double

    Returns linear regression slope of input values. ``y`` is the dependent
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

Lambda Aggregate Functions
--------------------------

.. function:: reduce_agg(inputValue T, initialState S, inputFunction(S, T, S), combineFunction(S, S, S)) -> S

    Reduces all input values into a single value. ``inputFunction`` will be invoked
    for each non-null input value. In addition to taking the input value, ``inputFunction``
    takes the current state, initially ``initialState``, and returns the new state.
    ``combineFunction`` will be invoked to combine two states into a new state.
    The final state is returned::

        SELECT id, reduce_agg(value, 0, (a, b) -> a + b, (a, b) -> a + b)
        FROM (
            VALUES
                (1, 3),
                (1, 4),
                (1, 5),
                (2, 6),
                (2, 7)
        ) AS t(id, value)
        GROUP BY id;
        -- (1, 12)
        -- (2, 13)

        SELECT id, reduce_agg(value, 1, (a, b) -> a * b, (a, b) -> a * b)
        FROM (
            VALUES
                (1, 3),
                (1, 4),
                (1, 5),
                (2, 6),
                (2, 7)
        ) AS t(id, value)
        GROUP BY id;
        -- (1, 60)
        -- (2, 42)

    The state type must be a boolean, integer, floating-point, or date/time/interval.
