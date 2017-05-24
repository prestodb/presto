===================
聚合函数
===================

聚合函数操作一个集合并计算出一个结果。

除了 :func:`count`， :func:`count_if` 和 :func:`approx_distinct` ，
所有的聚合函数都忽略空值(null)，并在没有输入或者输入都为空的时候返回空值。
例如 :func:`sum` 返回空值并不返回0， :func:`avg` 在计算总数时候会将空值包含进去。
``coalesce`` 这个函数可以用来将空值转换为0。

通用聚合函数
---------------------------

.. function:: avg(x) -> double

    Returns the average (arithmetic mean) of all input values.

.. function:: count(*) -> bigint

    返回计数个数。

.. function:: count(x) -> bigint

    返回所有非空值的计数个数。

.. function:: count_if(x) -> bigint

    Returns the number of ``TRUE`` input values.
    This function is equivalent to ``count(CASE WHEN x THEN 1 END)``.

.. function:: max(x) -> [和输入一样]

    返回所有输入值的最大值。
    
.. function:: min(x) -> [和输入一样]

    返回所有输入值的最小值。

.. function:: sum(x) -> [和输入一样]

    返回所有输入值的和。

近似聚合函数
-------------------------------

.. function:: approx_avg(x) -> varchar

    Returns the approximate average with bounded error at 99% confidence for
    all input values of ``x``.

.. function:: approx_distinct(x) -> bigint

    Returns the approximate number of distinct input values.
    This function provides an approximation of ``count(DISTINCT x)``.
    Zero is returned if all input values are null.

    This function uses HyperLogLog configured with 2048 buckets. It should
    produce a standard error of 2.3%, which is the standard deviation of the
    (approximately normal) error distribution over all possible sets. It does
    not guarantee an upper bound on the error for any specific input set.

.. function:: approx_percentile(x, p) -> [和输入一样]

    Returns the approximate percentile for all input values of ``x`` at the
    percentage ``p``. The value of ``p`` must be between zero and one and
    must be constant for all input rows.

.. function:: approx_percentile(x, w, p) -> [和输入一样]

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at the percentage ``p``. The weight must be
    an integer value of at least one. It is effectively a replication count for
    the value ``x`` in the percentile set. The value of ``p`` must be between
    zero and one and must be constant for all input rows.

统计聚合函数
-------------------------------

.. function:: stddev(x) -> double

    :func:`stddev_samp` 的别名。

.. function:: stddev_pop(x) -> double

    Returns the population standard deviation of all input values.

.. function:: stddev_samp(x) -> double

    Returns the sample standard deviation of all input values.

.. function:: variance(x) -> double

    :func:`var_samp` 的别名。

.. function:: var_pop(x) -> double

    Returns the population variance of all input values.

.. function:: var_samp(x) -> double

    Returns the sample variance of all input values.
