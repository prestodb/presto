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

.. function:: reduce_agg(inputValue T, initialState S, inputFunction(S,T,S), combineFunction(S,S,S)) -> S

    Reduces all input values into a single value. ```inputFunction`` will be invoked
    for each input value. In addition to taking the input value, ``inputFunction``
    takes the current state, initially ``initialState``, and returns the new state.
    ``combineFunction`` will be invoked to combine two states into a new state.
    The final state is returned::

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

    The state type must be a boolean, integer, floating-point, or date/time/interval.

.. function:: set_agg(x) -> array<[same as input]>

        Returns an array created from the distinct input ``x`` elements.

.. function:: set_union(array(T)) -> array(T)

    Returns an array of all the distinct values contained in each array of the input

    Example::

        SELECT set_union(elements)
        FROM (
            VALUES
                ARRAY[1, 2, 3],
                ARRAY[2, 3, 4]
        ) AS t(elements);

    Returns ARRAY[1, 2, 3, 4]

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

.. function:: map_union_sum(x(K,V)) -> map(K,V)

      Returns the union of all the input maps summing the values of matching keys in all
      the maps. All null values in the original maps are coalesced to 0.

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

.. function:: approx_percentile(x, percentage, accuracy) -> [same as x]

    As ``approx_percentile(x, percentage)``, but with a maximum rank error of
    ``accuracy``. The value of ``accuracy`` must be between zero and one
    (exclusive) and must be constant for all input rows. Note that a lower
    "accuracy" is really a lower error threshold, and thus more accurate. The
    default accuracy is ``0.01``.

.. function:: approx_percentile(x, percentages) -> array<[same as x]>

    Returns the approximate percentile for all input values of ``x`` at each of
    the specified percentages. Each element of the ``percentages`` array must be
    between zero and one, and the array must be constant for all input rows.

.. function:: approx_percentile(x, percentages, accuracy) -> array<[same as x]>

    As ``approx_percentile(x, percentages)``, but with a maximum rank error of
    ``accuracy``.

.. function:: approx_percentile(x, w, percentage) -> [same as x]

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at the percentage ``p``. The weight must be
    an integer value of at least one. It is effectively a replication count for
    the value ``x`` in the percentile set. The value of ``p`` must be between
    zero and one and must be constant for all input rows.

.. function:: approx_percentile(x, w, percentage, accuracy) -> [same as x]

    As ``approx_percentile(x, w, percentage)``, but with a maximum rank error of
    ``accuracy``.

.. function:: approx_percentile(x, w, percentages) -> array<[same as x]>

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at each of the given percentages specified
    in the array. The weight must be an integer value of at least one. It is
    effectively a replication count for the value ``x`` in the percentile set.
    Each element of the array must be between zero and one, and the array must
    be constant for all input rows.

.. function:: approx_percentile(x, w, percentages, accuracy) -> array<[same as x]>

    As ``approx_percentile(x, w, percentages)``, but with a maximum rank error of
    ``accuracy``.


.. function:: approx_set(x) -> HyperLogLog
    :noindex:

    See :doc:`hyperloglog`.

.. function:: merge(x) -> HyperLogLog
    :noindex:

    See :doc:`hyperloglog`.

.. function:: khyperloglog_agg(x) -> KHyperLogLog
    :noindex:

    See :doc:`khyperloglog`.

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
    for all ``value``\ s with a per-item weight of ``weight``.  The keys of the
    returned map are roughly the center of the bin, and the entry is the total
    weight of the bin.  The algorithm is based loosely on [BenHaimTomTov2010]_.

    ``buckets`` must be a ``bigint``. ``value`` and ``weight`` must be numeric.

.. function:: numeric_histogram(buckets, value) -> map<double, double>

    Computes an approximate histogram with up to ``buckets`` number of buckets
    for all ``value``\ s. This function is equivalent to the variant of
    :func:`numeric_histogram` that takes a ``weight``, with a per-item weight of ``1``.
    In this case, the total weight in the returned map is the count of items in the bin.


Statistical Aggregate Functions
-------------------------------

.. function:: corr(y, x) -> double

    Returns correlation coefficient of input values.

.. function:: covar_pop(y, x) -> double

    Returns the population covariance of input values.

.. function:: covar_samp(y, x) -> double

    Returns the sample covariance of input values.

.. function:: entropy(c) -> double

    Returns the log-2 entropy of count input-values.

    .. math::

        \mathrm{entropy}(c) = \sum_i \left[ {c_i \over \sum_j [c_j]} \log_2\left({\sum_j [c_j] \over c_i}\right) \right].

    ``c`` must be a ``bigint`` column of non-negative values.

    The function ignores any ``NULL`` count. If the sum of non-``NULL`` counts is 0,
    it returns 0.

.. function:: kurtosis(x) -> double

    Returns the excess kurtosis of all input values. Unbiased estimate using
    the following expression:

    .. math::

        \mathrm{kurtosis}(x) = {n(n+1) \over (n-1)(n-2)(n-3)} { \sum[(x_i-\mu)^4] \over \sigma^4} -3{ (n-1)^2 \over (n-2)(n-3) },

   where :math:`\mu` is the mean, and :math:`\sigma` is the standard deviation.

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


Classification Metrics Aggregate Functions
------------------------------------------

The following functions each measure how some metric of a binary
`confusion matrix <https://en.wikipedia.org/wiki/Confusion_matrix>`_ changes as a function of
classification thresholds. They are meant to be used in conjunction.

For example, to find the `precision-recall curve <https://en.wikipedia.org/wiki/Precision_and_recall>`_, use

    .. code-block:: none

         WITH
             recall_precision AS (
                 SELECT
                     CLASSIFICATION_RECALL(10000, correct, pred) AS recalls,
                     CLASSIFICATION_PRECISION(10000, correct, pred) AS precisions
                 FROM
                    classification_dataset
             )
         SELECT
             recall,
             precision
         FROM
             recall_precision
         CROSS JOIN UNNEST(recalls, precisions) AS t(recall, precision)

To get the corresponding thresholds for these values, use

    .. code-block:: none

         WITH
             recall_precision AS (
                 SELECT
                     CLASSIFICATION_THRESHOLDS(10000, correct, pred) AS thresholds,
                     CLASSIFICATION_RECALL(10000, correct, pred) AS recalls,
                     CLASSIFICATION_PRECISION(10000, correct, pred) AS precisions
                 FROM
                    classification_dataset
             )
         SELECT
             threshold,
             recall,
             precision
         FROM
             recall_precision
         CROSS JOIN UNNEST(thresholds, recalls, precisions) AS t(threshold, recall, precision)

To find the `ROC curve <https://en.wikipedia.org/wiki/Receiver_operating_characteristic>`_, use

    .. code-block:: none

         WITH
             fallout_recall AS (
                 SELECT
                     CLASSIFICATION_FALLOUT(10000, correct, pred) AS fallouts,
                     CLASSIFICATION_RECALL(10000, correct, pred) AS recalls
                 FROM
                    classification_dataset
             )
         SELECT
             fallout
             recall,
         FROM
             recall_fallout
         CROSS JOIN UNNEST(fallouts, recalls) AS t(fallout, recall)


.. function:: classification_miss_rate(buckets, y, x, weight) -> array<double>

    Computes the miss-rate with up to ``buckets`` number of buckets. Returns
    an array of miss-rate values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1; ``weight`` should be non-negative values, indicating the weight of the instance.

    The
    `miss-rate <https://en.wikipedia.org/wiki/Type_I_and_type_II_errors#False_positive_and_false_negative_rates>`_
    is defined as a sequence whose :math:`j`-th entry is

    .. math ::

        {
            \sum_{i \;|\; x_i \leq t_j \bigwedge y_i = 1} \left[ w_i \right]
            \over
            \sum_{i \;|\; x_i \leq t_j \bigwedge y_i = 1} \left[ w_i \right]
            +
            \sum_{i \;|\; x_i > t_j \bigwedge y_i = 1} \left[ w_i \right]
        },

    where :math:`t_j` is the :math:`j`-th smallest threshold,
    and :math:`y_i`, :math:`x_i`, and :math:`w_i` are the :math:`i`-th
    entries of ``y``, ``x``, and ``weight``, respectively.

.. function:: classification_miss_rate(buckets, y, x) -> array<double>

    This function is equivalent to the variant of
    :func:`classification_miss_rate` that takes a ``weight``, with a per-item weight of ``1``.

.. function:: classification_fall_out(buckets, y, x, weight) -> array<double>

    Computes the fall-out with up to ``buckets`` number of buckets. Returns
    an array of fall-out values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1; ``weight`` should be non-negative values, indicating the weight of the instance.

    The
    `fall-out <https://en.wikipedia.org/wiki/Information_retrieval#Fall-out>`_
    is defined as a sequence whose :math:`j`-th entry is

    .. math ::

        {
            \sum_{i \;|\; x_i > t_j \bigwedge y_i = 0} \left[ w_i \right]
            \over
            \sum_{i \;|\; y_i = 0} \left[ w_i \right]
        },

    where :math:`t_j` is the :math:`j`-th smallest threshold,
    and :math:`y_i`, :math:`x_i`, and :math:`w_i` are the :math:`i`-th
    entries of ``y``, ``x``, and ``weight``, respectively.

.. function:: classification_fall_out(buckets, y, x) -> array<double>

    This function is equivalent to the variant of
    :func:`classification_fall_out` that takes a ``weight``, with a per-item weight of ``1``.

.. function:: classification_precision(buckets, y, x, weight) -> array<double>

    Computes the precision with up to ``buckets`` number of buckets. Returns
    an array of precision values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1; ``weight`` should be non-negative values, indicating the weight of the instance.

    The
    `precision <https://en.wikipedia.org/wiki/Positive_and_negative_predictive_values>`_
    is defined as a sequence whose :math:`j`-th entry is

    .. math ::

        {
            \sum_{i \;|\; x_i > t_j \bigwedge y_i = 1} \left[ w_i \right]
            \over
            \sum_{i \;|\; x_i > t_j} \left[ w_i \right]
        },

    where :math:`t_j` is the :math:`j`-th smallest threshold,
    and :math:`y_i`, :math:`x_i`, and :math:`w_i` are the :math:`i`-th
    entries of ``y``, ``x``, and ``weight``, respectively.

.. function:: classification_precision(buckets, y, x) -> array<double>

    This function is equivalent to the variant of
    :func:`classification_precision` that takes a ``weight``, with a per-item weight of ``1``.

.. function:: classification_recall(buckets, y, x, weight) -> array<double>

    Computes the recall with up to ``buckets`` number of buckets. Returns
    an array of recall values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1; ``weight`` should be non-negative values, indicating the weight of the instance.

    The
    `recall <https://en.wikipedia.org/wiki/Precision_and_recall#Recall>`_
    is defined as a sequence whose :math:`j`-th entry is

    .. math ::

        {
            \sum_{i \;|\; x_i > t_j \bigwedge y_i = 1} \left[ w_i \right]
            \over
            \sum_{i \;|\; y_i = 1} \left[ w_i \right]
        },

    where :math:`t_j` is the :math:`j`-th smallest threshold,
    and :math:`y_i`, :math:`x_i`, and :math:`w_i` are the :math:`i`-th
    entries of ``y``, ``x``, and ``weight``, respectively.

.. function:: classification_recall(buckets, y, x) -> array<double>

    This function is equivalent to the variant of
    :func:`classification_recall` that takes a ``weight``, with a per-item weight of ``1``.

.. function:: classification_thresholds(buckets, y, x) -> array<double>

    Computes the thresholds with up to ``buckets`` number of buckets. Returns
    an array of threshold values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1.

    The thresholds are defined as a sequence whose :math:`j`-th entry is the :math:`j`-th smallest threshold.


Differential Entropy Functions
-------------------------------

The following functions approximate the binary `differential entropy <https://en.wikipedia.org/wiki/Differential_entropy>`_.
That is, for a random variable :math:`x`, they approximate

.. math ::

    h(x) = - \int x \log_2\left(f(x)\right) dx,

where :math:`f(x)` is the partial density function of :math:`x`.

.. function:: differential_entropy(sample_size, x)

    Returns the approximate log-2 differential entropy from a random variable's sample outcomes. The function internally
    creates a reservoir (see [Black2015]_), then calculates the
    entropy from the sample results by approximating the derivative of the cumulative distribution
    (see [Alizadeh2010]_).

    ``sample_size`` (``long``) is the maximal number of reservoir samples.

    ``x`` (``double``) is the samples.

    For example, to find the differential entropy of ``x`` of ``data`` using 1000000 reservoir samples, use

    .. code-block:: none

         SELECT
             differential_entropy(1000000, x)
         FROM
             data

    .. note::

        If :math:`x` has a known lower and upper bound,
        prefer the versions taking ``(bucket_count, x, 1.0, "fixed_histogram_mle", min, max)``,
        or ``(bucket_count, x, 1.0, "fixed_histogram_jacknife", min, max)``,
        as they have better convergence.

.. function:: differential_entropy(sample_size, x, weight)

    Returns the approximate log-2 differential entropy from a random variable's sample outcomes. The function
    internally creates a weighted reservoir (see [Efraimidis2006]_), then calculates the
    entropy from the sample results by approximating the derivative of the cumulative distribution
    (see [Alizadeh2010]_).

    ``sample_size`` is the maximal number of reservoir samples.

    ``x`` (``double``) is the samples.

    ``weight`` (``double``) is a non-negative double value indicating the weight of the sample.

    For example, to find the differential entropy of ``x`` with weights ``weight`` of ``data``
    using 1000000 reservoir samples, use

    .. code-block:: none

         SELECT
             differential_entropy(1000000, x, weight)
         FROM
             data

    .. note::

        If :math:`x` has a known lower and upper bound,
        prefer the versions taking ``(bucket_count, x, weight, "fixed_histogram_mle", min, max)``,
        or ``(bucket_count, x, weight, "fixed_histogram_jacknife", min, max)``,
        as they have better convergence.

.. function:: differential_entropy(bucket_count, x, weight, method, min, max) -> double

    Returns the approximate log-2 differential entropy from a random variable's sample outcomes. The function
    internally creates a conceptual histogram of the sample values, calculates the counts, and
    then approximates the entropy using maximum likelihood with or without Jacknife
    correction, based on the ``method`` parameter. If Jacknife correction (see [Beirlant2001]_) is used, the
    estimate is

    .. math ::

        n H(x) - (n - 1) \sum_{i = 1}^n H\left(x_{(i)}\right)

    where :math:`n` is the length of the sequence, and :math:`x_{(i)}` is the sequence with the :math:`i`-th element
    removed.

    ``bucket_count`` (``long``) determines the number of histogram buckets.

    ``x`` (``double``) is the samples.

    ``method`` (``varchar``) is either ``'fixed_histogram_mle'`` (for the maximum likelihood estimate)
    or ``'fixed_histogram_jacknife'`` (for the jacknife-corrected maximum likelihood estimate).

    ``min`` and ``max`` (both ``double``) are the minimal and maximal values, respectively;
    the function will throw if there is an input outside this range.

    ``weight`` (``double``) is the weight of the sample, and must be non-negative.

    For example, to find the differential entropy of ``x``, each between ``0.0`` and ``1.0``,
    with weights 1.0 of ``data`` using 1000000 bins and jacknife estimates, use

    .. code-block:: none

         SELECT
             differential_entropy(1000000, x, 1.0, 'fixed_histogram_jacknife', 0.0, 1.0)
         FROM
             data

    To find the differential entropy of ``x``, each between ``-2.0`` and ``2.0``,
    with weights ``weight`` of ``data`` using 1000000 buckets and maximum-likelihood estimates, use

        .. code-block:: none

             SELECT
                 differential_entropy(1000000, x, weight, 'fixed_histogram_mle', -2.0, 2.0)
             FROM
                 data

    .. note::

        If :math:`x` doesn't have known lower and upper bounds, prefer the versions taking ``(sample_size, x)``
        (unweighted case) or ``(sample_size, x, weight)`` (weighted case), as they use reservoir
        sampling which doesn't require a known range for samples.

        Otherwise, if the number of distinct weights is low,
        especially if the number of samples is low, consider using the version taking
        ``(bucket_count, x, weight, "fixed_histogram_jacknife", min, max)``, as jacknife bias correction,
        is better than maximum likelihood estimation. However, if the number of distinct weights is high,
        consider using the version taking ``(bucket_count, x, weight, "fixed_histogram_mle", min, max)``,
        as this will reduce memory and running time.

.. function:: approx_most_frequent(buckets, value, capacity) -> map<[same as value], bigint>

    Computes the top frequent values up to ``buckets`` elements approximately.
    Approximate estimation of the function enables us to pick up the frequent
    values with less memory. Larger ``capacity`` improves the accuracy of
    underlying algorithm with sacrificing the memory capacity. The returned
    value is a map containing the top elements with corresponding estimated
    frequency.

    The error of the function depends on the permutation of the values and its
    cardinality. We can set the capacity same as the cardinality of the
    underlying data to achieve the least error.

    ``buckets`` and ``capacity`` must be ``bigint``. ``value`` can be numeric
    or string type.

    The function uses the stream summary data structure proposed in the paper
    `Efficient computation of frequent and top-k elements in data streams <https://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf>`_ by A.Metwalley, D.Agrawl and A.Abbadi.


---------------------------

.. [Alizadeh2010] Alizadeh Noughabi, Hadi & Arghami, N. (2010). "A New Estimator of Entropy".

.. [Beirlant2001] Beirlant, Dudewicz, Gyorfi, and van der Meulen,
    "Nonparametric entropy estimation: an overview", (2001)

.. [BenHaimTomTov2010] Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
    J. Machine Learning Research 11 (2010), pp. 849--872.

.. [Black2015] Black, Paul E. (26 January 2015). "Reservoir sampling". Dictionary of Algorithms and Data Structures.

.. [Efraimidis2006] Efraimidis, Pavlos S.; Spirakis, Paul G. (2006-03-16). "Weighted random sampling with a reservoir".
    Information Processing Letters. 97 (5): 181–185.
