===================
Aggregate Functions
===================

Aggregate functions operate on a set of values to compute a single result.

Except for :func:`count`, :func:`count_if` and :func:`approx_distinct`, all
of these aggregate functions ignore null values and return null for no input
rows or when all values are null. For example, :func:`sum` returns null
rather than zero and :func:`avg` does include null values in the count.
The ``coalesce`` function can be used to convert null into zero.

General Aggregate Functions
---------------------------

.. function:: avg(x) -> double

    Returns the average (arithmetic mean) of all input values.

.. function:: count(*) -> bigint

    Returns the number of input rows.

.. function:: count(x) -> bigint

    Returns the number of non-null input values.

.. function:: count_if(x) -> bigint

    Returns the number of ``TRUE`` input values.
    This function is equivalent to ``count(CASE WHEN x THEN 1 END)``.

.. function:: max(x) -> bigint

    Returns the maximum value of all input values.

.. function:: min(x) -> bigint

    Returns the minimum value of all input values.

.. function:: sum(x) -> [same as input]

    Returns the sum of all input values.

Approximate Aggregate Functions
-------------------------------

.. function:: approx_distinct(x) -> bigint

    Returns the approximate number of distinct input values.
    This function provides an approximation of ``count(DISTINCT x)``.
    Zero is returned if all input values are null.

    This function uses HyperLogLog configured with 2048 buckets. It should
    produce a standard error of 2.3%, which is the standard deviation of the
    (approximately normal) error distribution over all possible sets. It does
    not guarantee an upper bound on the error for any specific input set.

.. function:: approx_percentile(x, p) -> [same as input]

    Returns the approximate percentile for all input values of ``x`` at the
    percentage ``p``. The value of ``p`` must be between zero and one and
    must be constant for all input rows.

.. function:: approx_percentile(x, w, p) -> [same as input]

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at the percentage ``p``. The weight must be
    an integer value of at least one. It is effectively a replication count for
    the value ``x`` in the percentile set. The value of ``p`` must be between
    zero and one and must be constant for all input rows.

Statistical Aggregate Functions
-------------------------------

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
